import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, FunctionTransformer, StandardScaler

from data_pipeline.open_data_raw_material_price.core import OpenDataRawMaterialPrice
from util.s3_manager.manager import S3Manager
import numpy as np


class Transformer:

    def __init__(self, strategy=None, df=None):
        """
        TODO : overwrite to self.strategy
        :param strategy:
        """

        if strategy is None:
            self.strategy = {
                "categorical_features": {
                    "one_hot_encoding": ['품목명', '조사지역명', 'is_weekend', 'season'],
                },
                "numerical_features": {
                    "log": [
                        "최저기온(°C)", "최대 풍속(m/s)", "평균 풍속(m/s)", "최소 상대습도(pct)",
                        "강수 계속시간(hr)", "평균 수온(°C)", "평균 최대 파고(m)", "평균 유의 파고(m)",
                        "최고 유의 파고(m)", "최고 최대 파고(m)", "평균 파주기(sec)", "최고 파주기(sec)"
                    ],
                    "standard": ["당일조사가격"],
                    "none": [
                        "평균기온(°C)", "최고기온(°C)", "일강수량(mm)",
                        "평균 상대습도(pct)", "평균기압(hPa)", "평균 기온(°C)"
                    ]
                }
            }
        else:
            self.strategy = strategy

        self.df = df
        # categorical columns
        categorical_features = self.strategy["categorical_features"]["one_hot_encoding"]

        # numerical columns
        numeric_features_log = self.strategy["numerical_features"]["log"]
        numeric_features_standard = self.strategy["numerical_features"]["standard"]
        numeric_features_none = self.strategy["numerical_features"]["none"]




    def header(self, df):
        print(df["품목명"].unique())
        x = df[self.strategy["categorical_features"]["one_hot_encoding"]].apply(lambda x: x.unique(), axis=1)
        print(x)

    def execute(self):
        return self.proccess()

    def proccess(self):
        transformed_df = self.column_transformer.fit_transform(self.df)
        return transformed_df


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
    t = Transformer(df=df)
    print(t.proccess())
    # t.header(df)


if __name__ == '__main__':
    main()
