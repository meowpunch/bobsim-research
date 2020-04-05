from pandas import get_dummies

from util.logging import init_logger
from util.s3_manager.manager import S3Manager
from util.transform import CustomTransformer


class RawMaterialPriceExtractor:
    def __init__(self, date: str):
        self.logger = init_logger()

        # s3
        self.bucket_name = "production-bobsim"
        self.load_key = "public_data/open_data_raw_material_price/process/csv/{filename}.csv".format(
            filename=date
        )

        # TODO: not loaded here. extractor just do preprocess data
        self.input_df = self.load()

    def load(self):
        """
            fetch DataFrame and astype and filter by columns
        :return: pd DataFrame
        """
        manager = S3Manager(bucket_name=self.bucket_name)
        df = manager.fetch_objects(key=self.load_key)

        # TODO: no use index to get first element.
        return df[0]

    def process(self):
        categorical_features = ['item_name', 'region', 'is_weekend', 'season']
        df = get_dummies(self.input_df, columns=categorical_features)
        return df, "date"
