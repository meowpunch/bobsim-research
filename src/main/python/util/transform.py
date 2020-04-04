import sys
from functools import reduce

import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, FunctionTransformer, StandardScaler

from util.build_dataset import build_process_weather, build_process_price, build_origin_price
from util.logging import init_logger
from util.s3_manager.manager import S3Manager


class CustomTransformer(ColumnTransformer):
    """
        overwrite sklearn ColumnTransformer
    """
    def __init__(self, strategy=None):
        """
        TODO : overwrite to self.strategy
        :param strategy: dictionary {
            "method": [] list of columns
        }
        """
        self.logger = init_logger()

        if strategy is None:
            self.strategy = {
                "one_hot_encoding": ['품목명', '조사지역명', 'is_weekend', 'season'],
                "log": [
                    "최저기온(°C)", "최대 풍속(m/s)", "평균 풍속(m/s)", "최소 상대습도(pct)",
                    "강수 계속시간(hr)", "평균 수온(°C)", "평균 최대 파고(m)", "평균 유의 파고(m)",
                    "최고 유의 파고(m)", "최고 최대 파고(m)", "평균 파주기(sec)", "최고 파주기(sec)"
                ],
                "standard": ["당일조사가격"],
            }
        else:
            self.strategy = strategy

        # self.input_df = input_df

        # constant
        self.transform_method = {
            "log": FunctionTransformer(np.log1p),
            "standard": StandardScaler(),
            "one_hot_encoding": OneHotEncoder(sparse=False),
        }

        # self.column_transformer = ColumnTransformer(transformers=self.make_transformers())
        self.transformed = None
        super().__init__(transformers=self.make_transformers())

    def make_transformers(self):
        # transformers for ColumnTransformer
        method_list = self.strategy.keys()

        def make_tuple(name):
            try:
                return tuple([name, self.transform_method[name], self.strategy[name]])
            except KeyError:
                self.logger.critical("'{}' is not supported method name".format(name), exc_info=True)
                # TODO: handle exception
                sys.exit()

        return list(map(make_tuple, method_list))

    @property
    def transformed_df(self):
        if self.transformed is None:
            raise Exception("Not processed")
        return pd.DataFrame(self.transformed, columns=self.header)

    @property
    def header(self):
        """
            It should be called after transformers are fitted
        :return: list of column name
        """
        if self.transformed is None:
            raise Exception("Not processed")

        def get_columns(method_name):
            # columns for method
            if method_name is "one_hot_encoding":
                return self.fitted_transformer(method_name).get_feature_names().tolist()
            else:
                return list(map(
                    lambda column: '{method}_{column}'.format(method=method_name, column=column),
                    self.strategy[method_name]
                ))

        columns_list = list(map(get_columns, self.strategy.keys()))
        return list(reduce(lambda x, y: x + y, columns_list))

    def fitted_transformer(self, method="one_hot_encoding"):
        """
            get fitted transformer
        :param method: str, e.g. "one_hot_encoding", "log", "standard"
        :return: transformer for method
        """

        filtered = filter(
            # ColumnTransformer.transformers_: fitted transformers
            lambda x: method in x, self.transformers_
        ).__next__()
        return filter(lambda x: isinstance(x, BaseEstimator), filtered).__next__()

    def fit_transform(self, X, y=None):
        if self.transformed is None:
            self.transformed = super().fit_transform(X)

        return self.transformed


def load(date):
    """
        fetch DataFrame and astype and filter by columns
    :return: pd DataFrame
    """
    manager = S3Manager(bucket_name="production-bobsim")
    df = manager.fetch_objects(
        key="public_data/open_data_raw_material_price/process/csv/{filename}.csv".format(filename=date)
    )

    # TODO: no use index to get first element.
    return df[0]


def main():
    """
        test for Transformer
    """
    df, key = build_origin_price(date="201908")
    print(df.info())
    t = CustomTransformer(
        strategy={
            "one_hot_encoding": ['품목명', '조사지역명'],
            "standard": ["당일조사가격"],
            # "hey": ['ㅎ']
        }
    )
    print(t.fit_transform(df))
    print(t.header)
    print(t.transformed_df)


if __name__ == '__main__':
    main()
