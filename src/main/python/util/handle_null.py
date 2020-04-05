import sys
from functools import reduce

import pandas as pd

from util.build_dataset import build_origin_weather
from util.logging import init_logger
from util.s3_manager.manager import S3Manager


class NullHandler:
    def __init__(self, strategy, df):
        # only using staticmethod, no init instance variables
        if strategy is not None:
            self.input_df = df
            self.strategy = strategy

            self.fillna_method = {
                "drop": self.fillna_with_drop,
                "zero": self.fillna_with_zero,
                "linear": self.fillna_with_linear,
            }

    @staticmethod
    def fillna_with_linear(df: pd.DataFrame):
        # fill nan by linear formula.
        return df.interpolate(method='linear', limit_direction='both')

    @staticmethod
    def fillna_with_zero(df: pd.DataFrame):
        return df.fillna(value=0)

    @staticmethod
    def fillna_with_drop(df: pd.DataFrame):
        return df.dropna(axis=0)

    def get_columns_list(self):
        # TODO: in order not to scan df twice, combine this method with fillnan
        if len(self.strategy.values()) is 1:
            return list(self.strategy.values())[0]
        else:
            return reduce(
                lambda x, y: x + y,
                self.strategy.values()
            )

    @staticmethod
    def fill_by_method(method, df):
        filled = method(df)
        if isinstance(filled, pd.Series):
            return filled.to_frame()
        return filled

    def fill_nan(self):
        """
        :return: list of pd DataFrame filled nan by each method
        """
        method_list = self.strategy.keys()

        return list(map(
            lambda name: self.fill_by_method(
                self.fillna_method[name], self.input_df.filter(items=self.strategy[name])
            ), method_list
        ))

    def process(self):
        """
            by strategy, fill nan in df at once
        :return: pd DataFrame after fill nan
        """
        df_list = self.fill_nan()

        return pd.concat(
            df_list + [self.input_df.drop(columns=self.get_columns_list(), axis=1)],
            axis=1, join="inner", keys='index'
        )


def load(filename="2014-2020"):
    """
        fetch DataFrame and astype and filter by columns
    :return: pd DataFrame
    """
    manager = S3Manager(bucket_name="production-bobsim")
    df = manager.fetch_objects(
        key="public_data/open_data_terrestrial_weather/origin/csv/{filename}.csv".format(filename=filename)
    )

    # TODO: no use index to get first element.
    return df[0]


def main():
    df, key = build_origin_weather(date="201908")
    print(df.info())
    t = NullHandler(
        strategy={
            "linear": ["최대 풍속(m/s)"],
            "drop": ["최저기온(°C)"]
        }, df=df
    )
    print(t.process())


if __name__ == '__main__':
    main()
