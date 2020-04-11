import pandas as pd
from sklearn.linear_model import LinearRegression

from util.s3_manager.manage import S3Manager


class LinearRegressionModel:
    def __init__(self, x_train, y_train):
        self.model = LinearRegression()

        self.x_train = x_train
        self.y_train = y_train

    def fit(self):
        self.model.fit(self.x_train, self.y_train)

    def predict(self, X):
        return self.model.predict(X=X)

    def score(self):
        return self.model.score(self.x_train, self.y_train)

    def save_model(self, bucket_name, key):
        manager = S3Manager(bucket_name=bucket_name)
        manager.save_dump(self.model, key=key)

    @property
    def coef_df(self):
        """
        :return: pd DataFrame
        """
        return pd.Series(self.model.coef_, index=self.x_train.columns).rename("coef").reset_index()

    def save_coef(self, bucket_name, key):
        S3Manager(bucket_name=bucket_name).save_df_to_csv(self.coef_df, key=key)
