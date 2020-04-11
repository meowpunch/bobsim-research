import tempfile

import pandas as pd
from joblib import dump
import numpy as np
from sklearn.linear_model import ElasticNet
from sklearn.metrics import make_scorer, mean_squared_error
from sklearn.model_selection import GridSearchCV, TimeSeriesSplit

from util.logging import init_logger
from util.s3_manager.manage import S3Manager


class ElasticNetModel:
    """
        tested
    """
    def __init__(self, x_train, y_train, param=None):
        if param is None:
            self.model = ElasticNet()
        else:
            self.model = ElasticNet(param)

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


class ElasticNetSearcher(GridSearchCV):
    """
        for research
    """

    def __init__(
            self, x_train, y_train, score=mean_squared_error,
            params=None
    ):
        if params is None:
            params = {
                "max_iter": [1, 5, 10],
                "alpha": [0, 0.00001, 0.0001, 0.001, 0.01, 0.1, 1, 10, 100],
                "l1_ratio": np.arange(0.0, 1.0, 0.1)
            }
        self.logger = init_logger()

        self.x_train = x_train
        self.y_train = y_train

        super().__init__(
            estimator=ElasticNet(),
            param_grid=params,
            scoring=make_scorer(score, greater_is_better=False),
            # we have to know the relationship before and after obviously, so n_splits: 2
            cv=TimeSeriesSplit(n_splits=2).split(self.x_train)
        )

    def fit(self, X=None, y=None, groups=None, **fit_params):
        super().fit(X=self.x_train, y=self.y_train)

    def analyze_metric(self, X, y):
        """
        :param X: test_x
        :param y: test_y
        :return: metric and coef
        """
        pred_y = self.predict(X=X)
        score = self.scoring(y=y, y_pred=pred_y)
        self.logger.info("coef:\n{coef}".format(
            coef=pd.Series(self.best_estimator_.coef_, index=X.columns)
        ))
        self.logger.info("customized RMSE is {score}".format(score=score))

    def save_model(self, bucket_name, key):
        # save best elastic net
        S3Manager(bucket_name=bucket_name).save_dump(self.best_estimator_, key=key)

    @property
    def coef_df(self):
        """
        :return: pd DataFrame
        """
        return pd.Series(self.best_estimator_.coef_, index=self.x_train.columns).rename("coef").reset_index()

    def save_coef(self, bucket_name, key):
        S3Manager(bucket_name=bucket_name).save_df_to_csv(self.coef_df, key=key)
