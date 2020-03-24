from sklearn.metrics import mean_squared_error

from feature_extraction_pipeline.food_materials_price.main import FeatureExtractionPipeline
from model.regression import RegressionModel
from util.logging import init_logger


class PriceModelPipeline:

    def __init__(self):
        self.logger = init_logger()
        pass

    def process(self):
        """
            TODO: logic comes here
        :return: undefined
        """
        # extract features
        feature_pipeline = FeatureExtractionPipeline()
        train_x, test_x, train_y, test_y = feature_pipeline.process()

        # fit model
        model = RegressionModel(train_x, train_y)
        model.fit()

        # R^2
        self.logger.info("score is {score}".format(score=model.score()))

        # make batch prediction
        predictions = model.predict(test_x)

        # MSE
        self.logger.info("loss is {loss}".format(loss=mean_squared_error(test_y, predictions)))

        # TODO: save predictions and model
