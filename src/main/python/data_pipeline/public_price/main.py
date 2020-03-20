from data_pipeline.public_price.processor import Processor


def main():
    """
        TODO:

    :return: pandas DataFrame (public price)
    """
    processor = Processor()
    df = processor.execute()
    return df


if __name__ == '__main__':
    main()
