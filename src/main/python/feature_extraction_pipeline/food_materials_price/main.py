from sklearn.model_selection import train_test_split

from data_pipeline.food_materials_price.pipeline import PriceDataPipeline
from feature_extraction_pipeline.food_materials_price.extractor import FeatureExtractor


class FeatureExtractionPipeline:
    """
        1. run data pipeline process in order to load data
        2. extract feature using FeatureExtractor
        3. save and return vectorized form of data
    """
    def __init__(self):
        pass

    @staticmethod
    def process():
        """
        :return: vectorized form of data
        """
        # load a prepared data
        prepared_data = PriceDataPipeline().process()

        # extract feature
        feature_extractor = FeatureExtractor(
            prepared_data=prepared_data.drop("당일조사가격", axis=1)
        )
        X = feature_extractor.transform()
        y = prepared_data["당일조사가격"]

        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, stratify=y
        )

        # TODO: save

        return X_train, X_test, y_train, y_test


def main():
    x = FeatureExtractionPipeline()
    x.process()


if __name__ == '__main__':
    main()
