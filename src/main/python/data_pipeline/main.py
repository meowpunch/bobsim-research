from data_pipeline.processor import Processor
from util.logging import init_logger


class DataPipeline:

    def __init__(self):
        self.price =
        self.t_

    def process(self):
        """
        :return: price, terrestrial_weather, marine_weather (pd DataFrame)
        """
        Processor(key="public_price")
        return self.price, self.t_weather, self.m_weather


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
