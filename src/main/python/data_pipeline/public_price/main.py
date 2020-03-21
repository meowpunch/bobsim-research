from data_pipeline.public_price.processor import Processor
from util.logging import init_logger


def main():
    """
        TODO:

    :return: pandas DataFrame (public price)
    """
    logger = init_logger()
    logger.info("start processing public_price")
    processor = Processor()
    df = processor.execute()
    logger.info("success processing public_price!")
    return df


if __name__ == '__main__':
    main()
