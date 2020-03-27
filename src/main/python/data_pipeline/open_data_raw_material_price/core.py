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
        self.load_key = "public_data/public_price/origin/csv/{filename}".format(
            filename=self.file_name
        )
        self.save_key = "public_data/public_price/process/csv/{filename}".format(
            filename=self.file_name
        )

        # load and filter by columns
        self.columns = [
            "조사일자", "조사구분명",
            "표준품목명", "조사가격품목명", "표준품종명", "조사가격품종명",
            "조사등급명", "조사단위명", "당일조사가격", "조사지역명"
        ]
        self.input_df = self.load()

        self.processed_df = None

        # 0(success) or 1(fail) about processing data
        self.exit_code = bool()

    def load(self):
        """
            init S3Manager instances and fetch objects
        :return: pd DataFrame
        """
        manager = S3Manager(bucket_name=self.bucket_name)
        df = manager.fetch_objects(key=self.load_key)

        self.logger.info("{num} files is loaded".format(num=len(df)))
        self.logger.info("load df from origin bucket")
        return df[0][self.columns]

    def count_null(self):
        """
        return: pd Series represents the number of null values by column
        """
        return self.input_df.isna().sum().reset_index(name='count')

    def clean(self):
        """
        :return: DataFrame cleaned by null value
        """
        df_null = self.count_null().apply(lambda x: x.count > 0, axis=1)
        print(df_null)

        if df_null['count'].sum() > 0:
            df_null.apply(
                lambda x: self.logger.info(
                    "{col} has {num} null values".format(
                        col=x['index'], num=x['count']
                    )), axis=1)
            # drop rows have null values.
            return self.input_df.dropna(axis=0)
        else:
            self.logger.info("no missing value at raw material price")
            return self.input_df

    @staticmethod
    def transform(df: pd.DataFrame):
        """
            get skew by numeric columns and log by skew
        :param df: cleaned pd DataFrame
        :return: transformed pd DataFrame
        """
        # get skew
        features_index = df.dtypes[df.dtypes != 'object'].index
        skew_features = df[features_index].apply(lambda x: skew(x))

        # log by skew
        # TODO: define threshold not just '1'
        skew_features_top = skew_features[skew_features > 1]
        df[skew_features_top.index] = np.log1p(df[skew_features_top.index])

    def process(self):
        """
            process
                clean null value
                transform as distribution of data
                save processed data to s3
            TODO: save to rdb
        :return: exit_code code (bool)
        """
        df = self.clean()

        self.transform(df)

        print(df.info())

        self.save_s3(df)
        # TODO: save to s3

        return self.exit_code

    def save_s3(self, df: pd.DataFrame):
        manager = S3Manager(bucket_name=self.bucket_name)
        manager.save_objects(to_save_df=df, key=self.save_key)
        self.logger.info("{} is saved to s3 bucket({}) ".format(self.file_name, self.bucket_name))

