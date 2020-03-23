from data_pipeline.main import DataPipeline
from data_pipeline.processor import Processor
from feature_extraction_pipeline.food_materials_price.extractor import FeatureExtractor


class FeatureExtractionPipeline:
    """
        1. run data pipeline process in order to load data
        2. extract feature using FeatureExtractor
        3. save and return vectorized form of data
    """
    def __init__(self):
        self.types = ['price', 'terrestrial_weather', 'marine_weather']

        # TODO: decide whether to put public data as instance variables.
        self.price = None
        self.terrestrial_weather = None
        self.marine_weather = None

        self.train_label = None

    def load(self):
        """
        :return: dictionary {str(name): pd DataFrame}
        """
        df_list = DataPipeline(
            args=self.types
        ).execute()
        return dict(zip(self.types, df_list))

    def process(self):
        """
        :return: vectorized form of data
        """
        # load public data processed
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

    price_df = ()
    return


if __name__ == '__main__':
    main()