from data_pipeline.processor import Processor
from util.logging import init_logger


def main(arg):
    """
        init Processor instance and execute process
    :return: pandas DataFrame (public price)
    """
    p = Processor(key=arg)
    df = p.execute()

    return df


if __name__ == '__main__':
    main(arg="public_terrestrial_weather")
