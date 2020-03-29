from pandas import np
from sklearn.linear_model import ElasticNet
from sklearn.model_selection import GridSearchCV


class ElasticNetModel:
    def __init__(self, m_type, x_train, y_train):
        self.model = ElasticNet()
        self.param_grid = {
            "max_iter": [1, 5, 10],
            "alpha": [0.0001, 0.001, 0.01, 0.1, 1, 10, 100],
            "l1_ratio": np.arange(0.0, 1.0, 0.1)
        }

        # self.searcher = GridSearchCV(
        #     estimator=self.model,
        #     param_grid=self.param_grid,
        #     scoring="r2", cv=10
        # )

        self.x_train = x_train
        self.y_train = y_train

    def fit(self):
        self.model.fit(self.x_train, self.y_train)

    def predict(self, x_test):
        return self.model.predict(x_test)

    def score(self):
        return self.model.score(self.x_train, self.y_train)
