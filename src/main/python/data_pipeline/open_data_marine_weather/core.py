from datetime import datetime
from io import StringIO

import numpy as np
import pandas as pd
from scipy.stats import skew

from data_pipeline.dtype import dtype, reduction_dtype
from data_pipeline.translate import translation
from util.handle_null import NullHandler
from util.logging import init_logger
from util.s3_manager.manage import S3Manager


class OpenDataMarineWeather:

    def __init__(self, bucket_name: str, date: str):
        self.logger = init_logger()

        # TODO: how to handle datetime?
        self.term = datetime.strptime(date, "%Y%m")

        # s3
        self.bucket_name = bucket_name
        self.file_name = "2014-2020.csv"
        self.load_key = "public_data/open_data_marine_weather/origin/csv/{filename}".format(
            filename=self.file_name
        )
        self.save_key = "public_data/open_data_marine_weather/process/csv/{filename}.csv".format(
            filename=date
        )

        # type
        self.dtypes = dtype["marine_weather"]
        self.translate = translation["marine_weather"]

        # fillna

        self.columns_with_linear = [
            "m_wind_spd_avg", "m_atm_press_avg", "m_rel_hmd_avg",
            "m_temper_avg", "m_water_temper_avg", "m_max_wave_h_avg",
            "m_sign_wave_h_avg", "m_sign_wave_h_high", "m_max_wave_h_high"
        ]
        self.columns_with_zero = ['m_wave_p_avg', 'm_wave_p_high']
        """
        self.columns_with_linear = [
            "m_atm_press_avg", "m_rel_hmd_avg",
            "m_temper_avg", "m_water_temper_avg", "m_max_wave_h_avg",
            "m_max_wave_h_high"
        ]
        """
        # self.columns_with_zero = ['m_wave_p_avg']
        self.columns_with_drop = ['date']

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
        # weather by divided 'region' (지점) will be used on average
        return df.groupby("date").mean().reset_index()

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
    def transform(df):
        columns_with_log = ["m_wave_p_high", "m_wind_spd_avg"]

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
            # transformed = self.transform(cleaned)
            self.save(cleaned)
        except IOError as e:
            # TODO: consider that it can repeat to save one more time
            self.logger.critical(e, exc_info=True)
            return 1

        self.logger.info("success to process marine weather")
        return 0

    def save(self, df: pd.DataFrame):
        manager = S3Manager(bucket_name=self.bucket_name)
        manager.save_df_to_csv(df=df, key=self.save_key)
