import sys

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

        # load and filter by columns
        self.columns = [
            "일시", "평균기온(°C)", "최저기온(°C)",
            "최고기온(°C)", "강수 계속시간(hr)", "일강수량(mm)",
            "최대 풍속(m/s)", "평균 풍속(m/s)", "최소 상대습도(pct)",
            "평균 상대습도(pct)", "합계 일조시간(hr)"
        ]
        try:
            self.input_df = self.load()
        except IndexError:
            self.logger.critical("there is no file to be loaded", exc_info=True)
            sys.exit()

    def load(self):
        """
            init S3Manager instances and fetch objects
        :return: pd DataFrame
        """
        manager = S3Manager(bucket_name=self.bucket_name)
        df = manager.fetch_objects(key=self.load_key)

        self.logger.info("{num} files is loaded".format(num=len(df)))
        self.logger.info("load df from origin bucket")
        return df[0][self.columns].query('조사일자')

    def save(self, df: pd.DataFrame):
        manager = S3Manager(bucket_name=self.bucket_name)
        manager.save_object(to_save_df=df, key=self.save_key)

    def clean(self):
        """
            clean DataFrame by no used columns and null value
        :return: cleaned DataFrame
        """
        filtered_df = self.input_df[self.input_df.조사구분명 == "소비자가격"]
        # pd Series represents the number of null values by column
        df_null = filtered_df.isna().sum()

        if df_null.sum() > 0:
            filtered = df_null[df_null.map(lambda x: x > 0)]
            self.logger.info(filtered)

            # drop rows have null values.
            return filtered_df.dropna(axis=0)
        else:
            self.logger.info("no missing value at raw material price")
            return filtered_df

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
        }.get(unit_name, 1)
        # default 1

    def transform(self, df: pd.DataFrame):
        """
            get skew by numeric columns and log by skew
        :param df: cleaned pd DataFrame
        :return: transformed pd DataFrame
        """
        # transform by unit
        transformed = df.assign(
            조사단위명=lambda r: r.조사단위명.map(
                lambda x: self.get_unit(x)
            )
        ).assign(
            당일조사가격=lambda x: x.당일조사가격 / x.조사단위명
        ).drop("조사단위명", axis=1)

        # get skew
        skew_feature = transformed["당일조사가격"].skew()

        # log by skew
        # TODO: define threshold not just '1'
        if abs(skew_feature) > 1:
            skewed_df = transformed.assign(당일조사가격=np.log1p(transformed["당일조사가격"]))
            return skewed_df
        else:
            return transformed

    def process(self):
        """
            process
                clean null value
                transform as distribution of data
                save processed data to s3
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
