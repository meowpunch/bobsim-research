
import sys
from datetime import datetime

import pandas as pd
import numpy as np
from pandas import Index
from scipy.stats import skew

from util.logging import init_logger
from util.s3_manager.manager import S3Manager


class OpenDataTerrestrialWeather:

    def __init__(self):
        self.logger = init_logger()

        # s3
        self.bucket_name = "production-bobsim"
        self.file_name = "2014-2020.csv"
        self.load_key = "public_data/open_data_terrestrial_weather/origin/csv/{filename}".format(
            filename=self.file_name
        )
        self.save_key = "public_data/open_data_terrestrial_weather/process/csv/{filename}".format(
            filename=self.file_name
        )

        # type
        self.dtypes = {
            "일시": "datetime64",
            "평균기온(°C)": "float16", "최저기온(°C)": "float16",
            "최고기온(°C)": "float16", "강수 계속시간(hr)": "float16",
            "일강수량(mm)": "float16", "최대 풍속(m/s)": "float16",
            "평균 풍속(m/s)": "float16", "최소 상대습도(pct)": "float16",
            "평균 상대습도(pct)": "float16", "합계 일조시간(hr)": "float16",
        }
        self.columns = self.dtypes.keys()

        # TODO: how to handle datetime? it will be parameterized
        self.term = datetime.strptime("201908", "%Y%m")

        # load filtered df
        df = self.load()
        mask = (df.일시.dt.year == self.term.year) & (df.일시.dt.month == self.term.month)
        self.input_df = df[mask]

    def load(self):
        """
            fetch DataFrame and astype and filter by columns
        :return: pd DataFrame
        """
        manager = S3Manager(bucket_name=self.bucket_name)
        df = manager.fetch_objects(key=self.load_key)

        # TODO: no use index to get first element.
        # filter by column and check types
        return df[0][self.columns].astype(dtype=self.dtypes)

    def save(self, df: pd.DataFrame):
        manager = S3Manager(bucket_name=self.bucket_name)
        manager.save_object(to_save_df=df, key=self.save_key)

    def clean(self, df):
        """
            clean DataFrame by no used columns and null value
        :return: cleaned DataFrame
        """
        # pd Series represents the number of null values by column
        df_null = df.isna().sum()
        print(df_null)

        if df_null.sum() > 0:
            filtered = df_null[df_null.map(lambda x: x > 0)]
            self.logger.info(filtered)

            # drop rows have null values.
            return df.dropna(axis=0)
        else:
            self.logger.info("no missing value at raw material price")
            return df

    @staticmethod
    def get_unit(unit_name):
        # TODO: handle no supported unit_name
        return {
            '20KG': 200, '1.2KG': 12, '8KG': 80, '1KG': 10, '1KG(단)': 10, '1KG(1단)': 10,
            '500G': 5, '200G': 2, '100G': 1,
            '10마리': 10, '2마리': 2, '1마리': 1,
            '10개': 10, '1개': 1,
            '1L': 10,
            '1속': 1,
            '1포기': 1,
        }.get(unit_name, 1)  # default 1

    def by_unit(self, df: pd.DataFrame):
        """
            transform unit
        :return: transformed pd DataFrame
        """
        return df.assign(
            조사단위명=lambda r: r.조사단위명.map(
                lambda x: self.get_unit(x)
            )
        ).assign(
            당일조사가격=lambda x: x.당일조사가격 / x.조사단위명
        ).drop("조사단위명", axis=1)

    @staticmethod
    def by_skew(df: pd.DataFrame):
        # get skew
        skew_feature = df["당일조사가격"].skew()
        # log by skew
        # TODO: define threshold not just '1'
        if abs(skew_feature) > 1:
            skewed_df = df.assign(당일조사가격=np.log1p(df["당일조사가격"]))
            return skewed_df
        else:
            return df

    def transform(self, df: pd.DataFrame):
        """
            get skew by numeric columns and log by skew
        :param df: cleaned pd DataFrame
        :return: transformed pd DataFrame
        """
        # transform by unit
        transformed = self.by_unit(df)
        # get skew
        return self.by_skew(transformed)

    def process(self):
        """
            process
                1. clean null value
                2. transform as distribution of data
                3. save processed data to s3
            TODO: save to rdb
        :return: exit_code code (bool)  0: success 1: fail
        """
        try:
            cleaned = self.clean()
            transformed = self.transform(cleaned)
            self.save(transformed)
        except Exception("fail to save") as e:
            # TODO: consider that it can repeat to save one more time
            self.logger.critical(e, exc_info=True)
            return 1

        self.logger.info("success to process raw material price")
        return 0
