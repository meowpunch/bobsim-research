import tempfile

from joblib import dump
import numpy as np
from sklearn.linear_model import ElasticNet
from sklearn.metrics import make_scorer, mean_squared_error
from sklearn.model_selection import GridSearchCV, TimeSeriesSplit

from util.s3_manager.manager import S3Manager


class ElasticNetSearcher:
    def __init__(self, x_train, y_train, score=mean_squared_error):
        self.x_train = x_train
        self.y_train = y_train

        self.model = ElasticNet()
        self.param_grid = {
            "max_iter": [1, 5, 10],  # , 50, 100, 500, 1000],
            "alpha": [0.0001, 0.001, 0.01, 0.1, 1, 10, 100],
            "l1_ratio": np.arange(0.0, 1.0, 0.1)
        }

        self.searcher = GridSearchCV(
            estimator=self.model,
            param_grid=self.param_grid,
            scoring=make_scorer(score, greater_is_better=False),
            cv=TimeSeriesSplit(n_splits=3).split(self.x_train)
        )

    def fit(self):
        self.searcher.fit(self.x_train, self.y_train)
        self.model = self.searcher.best_estimator_

    def predict(self, x_test):
        return self.searcher.predict(x_test)

    def score(self):
        return self.model.score(self.x_train, self.y_train)

    def save(self, bucket_name, key):
        with tempfile.TemporaryFile() as fp:
            dump(self.model, fp)
            fp.seek(0)
            manager = S3Manager(bucket_name=bucket_name)
            manager.save_object(body=fp.read(), key=key)
            fp.close()
