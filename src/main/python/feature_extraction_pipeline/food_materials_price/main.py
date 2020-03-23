from data_pipeline.food_materials_price.pipeline import PriceDataPipeline
from feature_extraction_pipeline.food_materials_price.extractor import FeatureExtractor


class FeatureExtractionPipeline:
    """
        1. run data pipeline process in order to load data
        2. extract feature using FeatureExtractor
        3. save and return vectorized form of data
    """

    def __init__(self):
        self.train_label = None

    @staticmethod
    def load(self):
        """
        :return: one prepared pd DataFrame
        """
        return PriceDataPipeline().execute()

    def process(self):
        """
        :return: vectorized form of data
        """
        # load public data running data-pipeline
        data = self.load()

        # extract feature
        feature_extractor = FeatureExtractor(
            price=data["price"],
            terrestrial_weather=data["terrestrial_weather"],
            marine_weather=data["marine_weather"],
        )
        train, test = feature_extractor.transform()

        # TODO: save

        return train, self.train_label, test


def main():
    # for test
    pass


if __name__ == '__main__':
    main()
