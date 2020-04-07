import tempfile

import pandas as pd
from joblib import dump
import numpy as np
from sklearn.linear_model import ElasticNet
from sklearn.metrics import make_scorer, mean_squared_error
from sklearn.model_selection import GridSearchCV, TimeSeriesSplit

from util.logging import init_logger
from util.s3_manager.manager import S3Manager


class ElasticNetModel:
    """
        tested
    """
    def __init__(self, x_train, y_train):
        self.model = ElasticNet(
            alpha=0, l1_ratio=0.0, max_iter=5
        )
        self.x_train = x_train
        self.y_train = y_train

    def fit(self):
        self.model.fit(self.x_train, self.y_train)

    def predict(self, x_test):
        return self.model.predict(x_test)

    def score(self):
        return self.model.score(self.x_train, self.y_train)

    def save(self, bucket_name, key):
        with tempfile.TemporaryFile() as fp:
            dump(self.model, fp)
            fp.seek(0)
            manager = S3Manager(bucket_name=bucket_name)
            manager.save_object(body=fp.read(), key=key)
            fp.close()


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


    # def fit(self):
    #     self.fit(self.x_train, self.y_train)
    #
    # def predict(self, x_test):
    #     return self.searcher.predict(x_test)
    #
    # def score(self):
    #     return self.model.score(self.x_train, self.y_train)

    def get_coef(self):
        return self.best_estimator_.coef_

    def save(self, bucket_name, key):
        # save best elastic net
        with tempfile.TemporaryFile() as fp:
            dump(self.best_estimator_, fp)
            fp.seek(0)
            manager = S3Manager(bucket_name=bucket_name)
            manager.save_object(body=fp.read(), key=key)
            fp.close()
