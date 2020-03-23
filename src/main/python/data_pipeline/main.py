import sys

from data_pipeline.processor import Processor
from util.logging import init_logger


class DataPipeline:
    """
        1. ingest data from public data portal
        2. process and save to rds & return pd DataFrames
    """

    def __init__(self, args):
        self.logger = init_logger()

        self.type_map = {
            "price": "public_price",
            "terrestrial_weather": "public_terrestrial_weather",
            "marine_weather": "public_marine_weather"
        }

        self.args = args
        self.types = None

        self.df_list = None

    def map_type(self, arg):
        try:
            return self.type_map[arg]
        except KeyError:
            raise KeyError("data pipeline does not support a type for {type}".format(type=arg))

    def execute(self):
        # TODO: ingest data

        try:
            self.types = list(map(self.map_type, self.args))

            self.df_list = list(map(
                lambda x: Processor(key=x).execute(),
                self.types))

            return self.df_list

        except KeyError as e:
            self.logger.critical(e, exc_info=True)
            sys.exit()


def main(arg):
    """
        init Processor instance and execute process
    :return: pandas DataFrame (public price)
    """
    p = Processor(key=arg)
    df = p.execute()

    return df


if __name__ == '__main__':
    main(arg="public_marine_weather")
