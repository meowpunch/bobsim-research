from feature_extraction_pipeline.food_materials_price.main import FeatureExtractionPipeline
from model.regression import RegressionModel


class PriceModelPipeline:

    def __init__(self):
        pass

    def process(self):
        """
            TODO: logic comes here
        :return: undefined
        """
        # extract features
        feature_pipeline = FeatureExtractionPipeline()
        train_x, train_y, test_x = feature_pipeline.process()

        # fit model
        model = RegressionModel(train_x, train_y)
        model.fit()

        # make batch prediction
        predictions = model.predict(test_x)
        test_y = None

        # TODO: save predictions and model

