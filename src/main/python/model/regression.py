from sklearn.linear_model import ElasticNet, LogisticRegression
from sklearn.neural_network import MLPRegressor


class RegressionModel:
    """
        TODO: Ridge, Lasso, Elastic.
    """
    def __init__(self, x_train, y_train):
        self.model = MLPRegressor()  # let's think about params
        self.x_train = x_train
        self.y_train = y_train

    def fit(self):
        self.model.fit(self.x_train, self.y_train)

    def predict(self, x_test):
        return self.model.predict(x_test)

    def score(self):
        return self.model.score(self.x_train, self.y_train)

