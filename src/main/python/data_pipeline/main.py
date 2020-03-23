from data_pipeline.processor import Processor
from util.logging import init_logger


class DataPipeline:
    """
        1. ingest data from public data portal
        2. process and save to rds & return pd DataFrames
    """

    def __init__(self):
        pass

    def process(self):
        """
        :return: price, terrestrial_weather, marine_weather (pd DataFrame)
        """
        # TODO: ingest data

        # process data
        p0 = Processor(key="public_price")
        price = p0.execute()
        p1 = Processor(key="public_terrestrial_weather")
        t_weather = p1.execute()
        p2 = Processor(key="public_marine_weather")
        m_weather = p2.execute()
        return price, t_weather, m_weather


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
