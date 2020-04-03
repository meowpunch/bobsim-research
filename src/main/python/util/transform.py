from functools import reduce

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, FunctionTransformer, StandardScaler

from util.s3_manager.manager import S3Manager


class Transformer:

    def __init__(self, strategy=None, input_df=None):
        """
        TODO : overwrite to self.strategy
        :param strategy:
        """
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

        self.transform_method = {
            "log": FunctionTransformer(np.log1p),
            "standard": StandardScaler(),
            "one_hot_encoding": OneHotEncoder(sparse=False),
        }

        self.input_df = input_df

        self.column_transformer = ColumnTransformer(transformers=self.make_transformers())

    def make_transformers(self):
        method_list = self.strategy.keys()
        return list(map(
            lambda name: tuple([name, self.transform_method[name], self.strategy[name]])
            , method_list
        ))

    def header(self):
        """
            # TODO: get header columns after transformed
        :return: list of column name
        """
        method_list = self.strategy.keys()
        print(self.column_transformer.transformers_)
        return self.column_transformer.get_feature_names()

    def process(self):
        return self.column_transformer.fit_transform(self.input_df)


def load(filename):
    """
        fetch DataFrame and astype and filter by columns
    :return: pd DataFrame
    """
    manager = S3Manager(bucket_name="production-bobsim")
    df = manager.fetch_objects(
        key="public_data/open_data_raw_material_price/process/csv/{filename}.csv".format(filename=filename)
    )

    # TODO: no use index to get first element.
    return df[0]

def main():
    df = load("201908")
    print(df)
    t = Transformer(
        strategy={
            "one_hot_encoding": ['품목명', '조사지역명', 'is_weekend', 'season'],
            "standard": ["당일조사가격"]
        }, input_df=df
    )
    print(pd.DataFrame(t.process()))
    print(t.header())
    # t.header(df)


if __name__ == '__main__':
    main()
