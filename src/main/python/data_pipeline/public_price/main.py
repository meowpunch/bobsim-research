from data_pipeline.public_price.processor import Processor
from util.logging import init_logger


def main():
    """
        init Processor instance and execute process
    :return: pandas DataFrame (public price)
    """
    processor = Processor(key="public_price")
    df = processor.execute()
    return df


if __name__ == '__main__':
    main()
