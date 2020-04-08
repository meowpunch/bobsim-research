from util.logging import init_logger
from util.s3_manager.manage import S3Manager


class MarineWeatherExtractor:
    def __init__(self, bucket_name: str, date: str):
        self.logger = init_logger()

        # s3
        self.bucket_name = bucket_name
        self.load_key = "public_data/open_data_marine_weather/process/csv/{filename}.csv".format(
            filename=date
        )

        self.input_df = self.load()
        self.categorical_features = []

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
        return self.input_df, "date"

