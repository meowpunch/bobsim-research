from sklearn.linear_model import ElasticNet


class RegressionModel:
    """
        TODO: Ridge, Lasso, Elastic.
    """
    def __init__(self, x_train, y_train):
        self.model = ElasticNet()  # let's think about params
        self.x_train = x_train
        self.y_train = y_train

    def fit(self, sample_weight=None):
        self.model.fit(self.x_train, self.y_train, sample_weight)

    def predict(self, x_test):
        return self.model.predict(X=x_test)

    def score(self):
        return self.model.score(self.x_train, self.y_train)

