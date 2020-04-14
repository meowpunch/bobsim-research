import pandas as pd

from analysis.filter_sparse import get_sparse_list, filter_sparse
from data_pipeline.dtype import dtype, reduction_dtype
from data_pipeline.translate import translation
from data_pipeline.unit import get_unit
from util.handle_null import NullHandler
from util.logging import init_logger
from util.s3_manager.manage import S3Manager
from util.visualize import draw_hist


class OpenDataRawMaterialPrice:

    def __init__(self, bucket_name: str, date: str):
        self.logger = init_logger()

        self.date = date

        # s3
        # TODO: bucket_name -> parameterized
        self.s3_manager = S3Manager(bucket_name=bucket_name)
        self.load_key = "public_data/open_data_raw_material_price/origin/csv/{filename}.csv".format(
            filename=self.date
        )
        self.save_key = "public_data/open_data_raw_material_price/process/csv/{filename}.csv".format(
            filename=self.date
        )

        self.dtypes = dtype["raw_material_price"]
        self.translate = translation["raw_material_price"]

        # load filtered df
        self.input_df = self.load()

    def load(self):
        """
            fetch DataFrame and check validate
        :return: pd DataFrame
        """
        # fetch
        df = self.s3_manager.fetch_objects(key=self.load_key)

        # TODO: no use index to get first element.
        # validate (filter by column and check types)
        return df[0][self.dtypes.keys()].astype(dtype=self.dtypes).rename(columns=self.translate, inplace=False)

    def save(self, df: pd.DataFrame):
        self.s3_manager.save_df_to_csv(df=df, key=self.save_key)

    def clean(self, df: pd.DataFrame):
        """
            clean null value
        :return: cleaned DataFrame
        """
        # pd Series represents the number of null values by column
        nh = NullHandler()
        df_null = nh.missing_values(df)
        self.logger.info("missing values: \n {}".format(df_null))

        if df_null is None:
            return df
        else:
            return df.dropna(axis=0)

    def standardize(self, s: pd.Series):
        mean, std = s.mean(), s.std()
        self.logger.info("{name}'s mean: {m}, std: {s}".format(name=s.name, m=mean, s=std))
        stdized = s.apply(lambda x: (x - mean) / std).rename("stdized_price")
        return stdized, mean, std

    def save_hist(self, s: pd.Series, key):
        draw_hist(s)
        self.s3_manager.save_plt_to_png(key=key)

    def transform(self, df: pd.DataFrame):
        """
            get skew by numeric columns and log by skew
        :param df: cleaned pd DataFrame
        :return: transformed pd DataFrame
        """
        origin_price = df["price"]
        self.save_hist(
            origin_price, key="food_material_price_predict_model/image/origin_price_hist_{d}.png".format(d=self.date)
        )

        stdized_price, mean, std = self.standardize(origin_price)
        self.save_hist(
            stdized_price, key="food_material_price_predict_model/image/stdized_price_hist_{d}.png".format(d=self.date)
        )

        self.s3_manager.save_dump(
            x=(mean, std), key="food_material_price_predict_model/price_(mean,std)_{date}.pkl".format(date=self.date))
        return df.assign(price=stdized_price)

    @staticmethod
    def combine_categories(df: pd.DataFrame):
        """
            combine 4 categories into one category 'item name'
        :return: combined pd DataFrame
        """
        return df.assign(
            item_name=lambda
                x: x.standard_item_name + x.survey_price_item_name + x.standard_breed_name + x.survey_price_type_name
        ).drop(
            columns=["standard_item_name", "survey_price_item_name", "standard_breed_name", "survey_price_type_name"],
            axis=1
        )

    @staticmethod
    def convert_by_unit(df: pd.DataFrame):
        """
            transform unit
        :return: transformed pd DataFrame
        """
        return df.assign(unit=lambda r: r.unit_name.map(
            lambda x: get_unit(x)
            # TODO: not unit but stardard unit name
        )).assign(price=lambda x: x.price / x.unit).drop(columns=["unit_name", "unit"], axis=1)

    def filter(self, df):
        """
            ready to process
        :param df: self.input_df
        :return: filtered pd DataFrame
        """
        # only retail price
        retail = df[df["class"] == "소비자가격"].drop("class", axis=1)

        convert = self.convert_by_unit(retail)
        sparse_list = get_sparse_list(df=convert, column="standard_item_name",
                                      number=48)
        replaced_df = filter_sparse(df=convert, column="standard_item_name",
                                    sparse_list=sparse_list)

        # combine 4 categories into one
        # combined = self.combine_categories(retail)

        # prices divided by 'material grade'(grade) will be used on average.
        aggregated = replaced_df.drop(["grade", "unit_name"], axis=1).groupby(
            ["date", "region", "standard_item_name"]  # "item_name"]
        ).mean().reset_index()

        # convert prices in standard unit
        return aggregated

    def process(self):
        """
            process
                1. filter
                2. clean null value
                3. transform as distribution of data
                4. add 'season' and 'is_weekend" column

                5. save processed data to s3
            TODO: save to rdb
        :return: exit code (bool)  0:success 1:fail
        """
        try:
            filtered = self.filter(self.input_df)
            cleaned = self.clean(filtered)
            transformed = self.transform(cleaned)

            # decomposed = self.decompose_date(transformed)

            self.save(transformed)
        except IOError as e:
            # TODO: consider that it can repeat to save one more time
            self.logger.critical(e, exc_info=True)
            return 1

        self.logger.info("success to process raw material price")
        return 0

    @staticmethod
    def decompose_date(df: pd.DataFrame):
        # TODO: do by argument
        # add is_weekend & season column
        return df.assign(
            is_weekend=lambda x: x["date"].dt.dayofweek.apply(
                lambda day: 1 if day > 4 else 0
            ),
            season=lambda x: x["date"].dt.month.apply(
                lambda month: (month % 12 + 3) // 3
            )
        )
