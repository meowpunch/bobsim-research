from datetime import datetime
from io import StringIO

import numpy as np
import pandas as pd
from scipy.stats import skew

from data_pipeline.dtype import dtype
from util.handle_null import NullHandler
from util.logging import init_logger
from util.s3_manager.manager import S3Manager


class OpenDataTerrestrialWeather:

    def __init__(self, date: str):
        self.logger = init_logger()

        # TODO: how to handle datetime?
        self.term = datetime.strptime(date, "%Y%m")

        # s3
        self.bucket_name = "production-bobsim"
        self.file_name = "2014-2020.csv"
        self.load_key = "public_data/open_data_terrestrial_weather/origin/csv/{filename}".format(
            filename=self.file_name
        )
        self.save_key = "public_data/open_data_terrestrial_weather/process/csv/{filename}.csv".format(
            filename=date
        )

        # type
        self.dtypes = dtype["terrestrial_weather"]

        # fillna
        self.columns_with_linear = ['평균기온(°C)', '최저기온(°C)', '최고기온(°C)', '최대 풍속(m/s)',
                                    '평균 풍속(m/s)', '최소 상대습도(pct)', '평균 상대습도(pct)']
        self.columns_with_zero = ['강수 계속시간(hr)', '일강수량(mm)']

        # log transformation
        self.columns_with_log = [
            '최저기온(°C)', '최고기온(°C)', '최소 상대습도(pct)',
            '평균 상대습도(pct)', '강수 계속시간(hr)', '일강수량(mm)'
        ]

        # load filtered df and take certain term
        df = self.load()
        # TODO: make function
        self.input_df = df[
            (df.일시.dt.year == self.term.year) & (df.일시.dt.month == self.term.month)
            ]

    def load(self):
        """
            fetch DataFrame and astype and filter by columns
        :return: pd DataFrame
        """
        manager = S3Manager(bucket_name=self.bucket_name)
        df = manager.fetch_objects(key=self.load_key)

        # TODO: no use index to get first element.
        # filter by column and check types
        return df[0][self.dtypes.keys()].astype(dtype=self.dtypes)

    def save(self, df: pd.DataFrame):
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        manager = S3Manager(bucket_name=self.bucket_name)
        manager.save_object(body=csv_buffer.getvalue().encode('euc-kr'), key=self.save_key)

    @staticmethod
    def groupby_date(df: pd.DataFrame):
        # weather by divided 'region' (지점) will be used on average
        return df.groupby(["일시"]).mean().reset_index()

    def clean(self, df: pd.DataFrame):
        """
            1. fillna with zero
            2. groupby "date" mean of "region"s
            3. fillna with linear
        :return: cleaned DataFrame
        """
        # null handler
        nh = NullHandler()

        # fill na
        filled_with_linear = nh.fillna_with_linear(
            self.groupby_date(df[self.columns_with_linear])
        )
        filled_with_zero = self.groupby_date(nh.fillna_with_zero(
            df[self.columns_with_zero]
        ))

        return pd.concat([df.drop(
            columns=self.columns_with_linear + self.columns_with_zero, axis=1
        ), filled_with_linear, filled_with_zero], axis=1)

    def transform_by_skew(self, df: pd.DataFrame):
        """
            get skew by numeric columns and log by skew
        :param df: cleaned pd DataFrame
        :return: transformed pd DataFrame
        """
        # numerical values remain
        filtered = df.dtypes[df.dtypes != "datetime64[ns]"].index

        # get skew
        skew_features = df[filtered].apply(lambda x: skew(x))

        # log by skew
        # TODO: define threshold not just '1'
        skew_features_top = skew_features[skew_features > 1]

        return pd.concat(
            [df.drop(columns=self.columns_with_log), np.log1p(df[self.columns_with_log])], axis=1
        )

    def process(self):
        """
            process
                1. clean
                2. transform as distribution of data
                3. save processed data to s3
            TODO: save to rdb
        :return: exit_code (bool)  0:success 1:fail
        """
        try:
            cleaned = self.clean(self.input_df)
            # transformed = self.transform_by_skew(cleaned)
            self.save(cleaned)
        except Exception as e:
            # TODO: consider that it can repeat to save one more time
            self.logger.critical(e, exc_info=True)
            return 1

        self.logger.info("success to process")
        return 0
