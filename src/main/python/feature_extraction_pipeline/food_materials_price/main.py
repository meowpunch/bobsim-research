from sklearn.model_selection import train_test_split

from data_pipeline.food_materials_price.pipeline import PriceDataPipeline
from feature_extraction_pipeline.food_materials_price.extractor import FeatureExtractor
from util.logging import init_logger


class FeatureExtractionPipeline:
    """
        1. run data pipeline process in order to load data
        2. extract feature using FeatureExtractor
        3. save and return vectorized form of data
    """
    def __init__(self):
        self.logger = init_logger()
        # load a prepared data and split test & train
        prepared_data = PriceDataPipeline().process()
        X = prepared_data.drop("당일조사가격", axis=1, inplace=False)
        y = prepared_data["당일조사가격"]

        self.logger.debug(X.groupby("품목명").count().sort_values(by=["평균기온(°C)"]))
        self.X_train, self.X_test, self.y_train, self.y_test = train_test_split(
            X, y, test_size=0.2, stratify=X["품목명"]
        )

    def process(self):
        """
        :return: vectorized form of data
        """
        # extract feature
        feature_extractor = FeatureExtractor(
            X_train=self.X_train, X_test=self.X_test
        )
        X_train, X_test = feature_extractor.fit_transform()

        # TODO: save

        return X_train, X_test, self.y_train, self.y_test


def main():
    x = FeatureExtractionPipeline()
    x.process()


if __name__ == '__main__':
    main()
