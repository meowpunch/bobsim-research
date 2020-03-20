import sys

from data_pipeline.public_price.loader import Loader
import logging

from util.logging import init_logger
from util.s3_manager.manager import S3Manager


class Processor:
    """
        origin bucket (s3) -> process bucket (rds_public_data, staging schema)
    """

    def __init__(self):
        self.df = None
        self.logger = init_logger()

    @staticmethod
    def load():
        """

        :return: pandas DataFrame (origin)
        """
        manager = S3Manager(bucket_name='production-bobsim',
                            key='public_price/origin/csv')
        df = manager.fetch_objects()
        # manager.fetch_objects_list()
        return df

    def validate(self):
        """
            validate data in origin bucket
            1. how to handling null_value

            # SAVE RDS -> Auto type checking??
        :return: validity (bool)
        """
        origin_df = self.load()

        return False

    @staticmethod
    def save(self):
        """
            save validated data to RDS
        :return: success or fail (bool)
        """
        return False

    def execute(self):

        b = self.validate()

        if b:
            s = self.save()
        else:
            self.logger.critical("validate fail")
            sys.exit()

        if s:
            return self.df
        else:
            self.logger.critical("save fail")
            sys.exit()
