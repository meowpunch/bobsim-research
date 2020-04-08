import sys
import tempfile

import numpy as np
import pandas as pd
from joblib import load, dump
from sklearn.base import BaseEstimator
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, FunctionTransformer, StandardScaler

from util.logging import init_logger
from util.reduce import combine_list
from util.s3_manager.manager import S3Manager


# TODO: not working, make available
def save(self):
    dump(self, 'custom_transformer.bin', compress=True)


def save_to_s3(transformer, bucket_name, key):
    with tempfile.TemporaryFile() as fp:
        dump(transformer, fp)
        fp.seek(0)
        S3Manager(bucket_name=bucket_name).save_object(body=fp.read(), key=key)
        fp.close()


def load_from_s3(bucket_name, key):
    with tempfile.TemporaryFile() as fp:
        S3Manager(bucket_name=bucket_name).s3_bucket.download_fileobj(Fileobj=fp, Key=key)
        fp.seek(0)
        transformer = load(fp)
        fp.close()
    return transformer


class CustomTransformer(ColumnTransformer):
    """
        overwrite sklearn ColumnTransformer
    """

    def __init__(self, df=None, strategy: dict = None):
        """
        TODO : overwrite to self.strategy
        :param strategy: dictionary {
            "method": [] list of columns
        }
        """
        if df is not None and strategy is not None:
            self.logger = init_logger()

            self.input_df = df
            self.strategy = strategy

            # constant
            self.transform_method = {
                "log": FunctionTransformer(np.log1p),
                "standard": StandardScaler(),
                "one_hot_encoding": OneHotEncoder(sparse=False),
                "none": FunctionTransformer(lambda x: x)
            }

            # self.column_transformer = ColumnTransformer(transformers=self.make_transformers())
            self.transformed = None
            super().__init__(transformers=self.make_transformers())
        else:
            super().__init__()

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
            raise Exception("Not transformed")
        return pd.DataFrame(self.transformed, columns=self.header)

    @property
    def header(self):
        """
            It should be called after transformers are fitted
        :return: list of column named
        """
        if self.transformed is None:
            raise Exception("Not transformed")

        def get_columns(method_name):
            # columns for method
            if method_name is "one_hot_encoding":
                return self.fitted_transformer(method_name).get_feature_names().tolist()
            else:
                return list(map(
                    lambda column: '{method}_{column}'.format(method=method_name, column=column),
                    self.strategy[method_name]
                ))

        # transformed columns
        columns_list = list(map(get_columns, self.strategy.keys()))
        return combine_list(columns_list)

    # override
    def fit(self, X=None, y=None):
        if X is None:
            X = self.input_df

        return super().fit(X)

    # override
    def fit_transform(self, X: pd.DataFrame = None, y=None):
        if X is None:
            X = self.input_df

        # TODO: use only once?
        if self.transformed is None:
            self.transformed = super().fit_transform(X)
            return self.transformed

        return self.transformed

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


def main():
    """
        test for Transformer
    """

    # df, key = build_origin_price(date="201908", prefix="clean")
    # print(df.info())
    # t = CustomTransformer(
    #     strategy={
    #         "one_hot_encoding": ['품목명', '조사지역명'],
    #         "standard": ["당일조사가격"],
    #         # "hey": ['ㅎ']
    #     }, df=df
    # )
    # print(t.fit_transform())
    # print(t.header)
    # print(t.transformed_df)

    pass


if __name__ == '__main__':
    main()
