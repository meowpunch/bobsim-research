from data_pipeline.public_price.processor import Processor
from util.logging import init_logger


def main():
    """
        TODO:

    :return: pandas DataFrame (public price)
    """
    init_logger().info("start processing")

    processor = Processor()
    df = processor.execute()
    return df


if __name__ == '__main__':
    main()
