import sys

import pandas as pd
import numpy as np
from pandas import Index
from scipy.stats import skew

from util.logging import init_logger
from util.s3_manager.manager import S3Manager


class OpenDataRawMaterialPrice:

    def __init__(self):
        self.logger = init_logger()

        # s3
        self.bucket_name = "production-bobsim"
        self.file_name = "201908.csv"
        self.load_key = "public_data/open_data_raw_material_price/origin/csv/{filename}".format(
            filename=self.file_name
        )
        self.save_key = "public_data/open_data_raw_material_price/process/csv/{filename}".format(
            filename=self.file_name
        )

        # type
        self.dtypes = {
            "조사일자": "datetime64",
            "조사구분명": "object", "표준품목명": "object", "조사가격품목명": "object", "표준품종명": "object",
            "조사가격품종명": "object", "조사등급명": "object", "조사단위명": "object",
            # TODO: type casting error btw UInt and float while aggregate mean
            "당일조사가격": "int",
            "조사지역명": "object",
        }
        self.columns = self.dtypes.keys()

        # load filtered df
        df = self.load()
        self.input_df = df[df.조사구분명 == "소비자가격"]

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

    def combine_categories(self):
        """
            starting point of process
            combine categories into one category
        :return: combined pd DataFrame
        """
        return self.input_df.assign(
            품목명=lambda x: x.표준품목명 + x.조사가격품목명 + x.표준품종명 + x.조사가격품종명
        ).drop(columns=["표준품목명", "조사가격품목명", "표준품종명", "조사가격품종명"], axis=1)

    def process(self):
        """
            process
                1. combine categories
                2. clean null value
                3. transform as distribution of data
                4. save processed data to s3
            TODO: save to rdb
        :return: exit_code code (bool)  0: success 1: fail
        """
        try:
            combined = self.combine_categories()
            cleaned = self.clean(combined)
            transformed = self.transform(cleaned)
            self.save(transformed)
        except Exception("fail to save") as e:
            # TODO: consider that it can repeat to save one more time
            self.logger.critical(e, exc_info=True)
            return 1

        self.logger.info("success to process raw material price")
        return 0
