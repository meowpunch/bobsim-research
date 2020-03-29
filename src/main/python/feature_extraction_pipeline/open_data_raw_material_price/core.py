from pandas import get_dummies

from util.logging import init_logger
from util.s3_manager.manager import S3Manager


class RawMaterialPriceExtractor:
    def __init__(self, date: str):
        self.logger = init_logger()

        # s3
        self.bucket_name = "production-bobsim"
        self.load_key = "public_data/open_data_raw_material_price/process/csv/{filename}.csv".format(
            filename=date
        )

        self.input_df = self.load()
        self.categorical_features = ['품목명', '조사지역명', 'is_weekend', 'season']

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
        df = get_dummies(self.input_df, columns=self.categorical_features)
        return df, "조사일자"
