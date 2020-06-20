from datetime import datetime
from io import StringIO

import numpy as np
import pandas as pd
from scipy.stats import skew

from data_pipeline.dtype import dtype, reduction_dtype
from data_pipeline.translate import translation
from utils.handle_null import NullHandler
from utils.logging import init_logger
from utils.s3_manager.manage import S3Manager


class OpenDataTerrestrialWeather:

    def __init__(self, bucket_name: str, date: str):
        self.logger = init_logger()

        # TODO: how to handle datetime?
        self.term = datetime.strptime(date, "%Y%m")

        # s3
        self.bucket_name = bucket_name
        self.file_name = "2014-2020.csv"
        self.load_key = "public_data/open_data_terrestrial_weather/origin/csv/{filename}".format(
            filename=self.file_name
        )
        self.save_key = "public_data/open_data_terrestrial_weather/process/csv/{filename}.csv".format(
            filename=date
        )

        # type
        self.dtypes = dtype["terrestrial_weather"]
        self.translate = translation["terrestrial_weather"]

        # fillna

        self.columns_with_linear = ['t_temper_avg', 't_temper_lowest', 't_temper_high', 't_wind_spd_max',
                                    't_wind_spd_avg', 't_rel_hmd_min', 't_rel_hmd_avg']
        self.columns_with_zero = ['t_dur_preci', 't_daily_preci']
        """
        self.columns_with_linear = ['t_temper_lowest', 't_rel_hmd_min']
        self.columns_with_zero = ['t_daily_preci']
        """
        self.columns_with_drop = ["date"]

        # load filtered df and take certain term
        df = self.load()
        # TODO: make function
        date_picker = (df['date'].dt.year == self.term.year) & (df['date'].dt.month == self.term.month)
        self.input_df = df[date_picker]

    def load(self):
        """
            fetch DataFrame and astype and filter by columns
        :return: pd DataFrame
        """
        manager = S3Manager(bucket_name=self.bucket_name)
        df = manager.fetch_df_from_csv(key=self.load_key)

        # TODO: no use index to get first element.
        # filter by column and check types
        return df[0][self.dtypes.keys()].astype(dtype=self.dtypes).rename(columns=self.translate, inplace=False)

    @staticmethod
    def groupby_date(df: pd.DataFrame):
        # weather by divided 'region' (t_location) will be used on average
        return df.groupby(["date"]).mean().reset_index()

    def clean(self, df: pd.DataFrame):
        """
        :return: cleaned DataFrame
        """
        # null handler (drop, zero)
        nh = NullHandler(
            strategy={"drop": self.columns_with_drop, "zero": self.columns_with_zero},
            df=df[self.columns_with_drop + self.columns_with_zero]
        )

        # groupby -> fillna (linear)
        linear = nh.fillna_with_linear(
            self.groupby_date(df)[self.columns_with_linear]
        )
        # fillna -> groupby (drop, zero)
        drop_and_zero = self.groupby_date(nh.process())

        return pd.concat([drop_and_zero, linear], axis=1)

    @staticmethod
    def transform(df: pd.DataFrame):
        # columns_with_log = ['t_daily_preci', 't_temper_avg', 't_temper_high']
        columns_with_log = ['t_daily_preci']

        return pd.concat([
            df.drop(columns=columns_with_log), np.log1p(df[columns_with_log])
        ], axis=1)

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
            transformed = self.transform(cleaned)
            # decomposed = self.decompose_precipitation(transformed)

            self.save(transformed)
        except IOError as e:
            # TODO: consider that it can repeat to save one more time
            self.logger.critical(e, exc_info=True)
            return 1

        self.logger.info("success to process terrestrial weather")
        return 0

    def save(self, df: pd.DataFrame):
        manager = S3Manager(bucket_name=self.bucket_name)
        manager.save_df_to_csv(df=df, key=self.save_key)

    @staticmethod
    def decompose_precipitation(df: pd.DataFrame):
        """
            This makes column presence or absence of precipitation
            During one month, the average national precipitation is greater than zero.
            So, This is meaningful if you do not groupby region for the national average.
        """
        return df.assign(
            t_preci_presence=lambda x: 0 if x.t_dur_preci is 0 and x.t_daily_preci is 0 else 1
        )