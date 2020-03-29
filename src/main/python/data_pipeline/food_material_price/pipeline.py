import pandas as pd

from data_pipeline.food_material_price.columns import condition
from data_pipeline.main import DataPipeline


class PriceDataPipeline:
    """
        prepare data from process data
        return prepared data(one pd DataFrame) for FeatureExtraction
    """

    def __init__(self):
        self.types = ['price', 'terrestrial_weather', 'marine_weather']

        # load processed data
        process_data = self.load()
        self.price = process_data["price"]
        self.t_weather = process_data["terrestrial_weather"]
        self.m_weather = process_data["marine_weather"]

    def load(self):
        """
            load and filter by column
        :return: dictionary {str(name): pd DataFrame}
        """
        df_list = DataPipeline(
            args=self.types
        ).execute()

        df_dict = dict(zip(self.types, df_list))

        def filter_by_column(x):
            df = df_dict[x]
            return df[condition[x]]

        filtered = list(map(filter_by_column, self.types))
        return dict(zip(self.types, filtered))

    def process(self):
        """
            filter, groupby and aggregate for price & weather
            combine price & weather and finally return
        :return: a prepared data (pd DataFrame)
        """
        # manipulate price
        tmp_p = self.price[self.price.apply(lambda x: x.조사구분명 == "소비자가격",
                                            axis=1)].drop("조사구분명", axis=1)
        filtered_p = tmp_p.assign(
            품목명=lambda x: x.표준품목명 + x.조사가격품목명 + x.표준품종명 + x.조사가격품종명
        ).drop(columns=["표준품목명", "조사가격품목명", "표준품종명", "조사가격품종명"], axis=1)

        price = filtered_p.groupby([
            "조사일자", "품목명", "조사지역명"
        ]).mean().reset_index()

        # manipulate weather
        weather = pd.merge(
            self.t_weather.groupby(["일시"]).mean(),
            self.m_weather.groupby(["일시"]).mean(),
            how='inner', on="일시"
        ).reset_index()

        # combine price with weather and return
        return pd.merge(
            price, weather,
            how="left", left_on="조사일자", right_on="일시"
        ).drop("일시", axis=1).astype(dtype={"조사일자": "datetime64"})
