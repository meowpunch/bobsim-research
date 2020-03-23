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
    def load():
        """
        :return: one prepared pd DataFrame
        """
        return PriceDataPipeline().process()

    def process(self):
        """
        :return: vectorized form of data
        """
        # load a prepared data
        prepared_data = self.load()
        print(prepared_data)

        # extract feature
        feature_extractor = FeatureExtractor(
            prepared_data=prepared_data
        )
        train, test = feature_extractor.transform()

        # TODO: save

        return train, self.train_label, test


def main():
    # for test
    pass


if __name__ == '__main__':
    main()
