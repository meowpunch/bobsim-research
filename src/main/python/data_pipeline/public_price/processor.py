import sys

from data_pipeline.public_price.loader import Loader
import logging


class Processor:
    """

    """

    def __init__(self):
        self.x = ''
        self.df = None

    def load(self):
        loader = Loader()
        return loader.execute()

    def validate(self):
        """
            validate data in origin bucket

            String:

            Number:
        :return: validity (bool)
        """
        origin_df = self.load()

        ...
        return True

    def save(self):
        """
            save validated data to process bucket
        :return: success or fail (bool)
        """
        return False

    def execute(self):

        p_logger = logging.getLogger("Processor")
        b = self.validate()

        if b:
            s = self.save()
        else:
            p_logger.warning("Validate fail")
            sys.exit()

        if s:
            return self.df
        else:
            p_logger.warning("Save fail")
            sys.exit()