import sys

from data_pipeline.open_data_raw_material_price.core import OpenDataRawMaterialPrice
from feature_extraction_pipeline.open_data_raw_material_price.core import RawMaterialPriceExtractor
from util.logging import init_logger


class RawMaterialPriceExtractionPipeline:
    def __init__(self, date: str):
        self.date = date

        self.logger = init_logger()

    @property
    def feature_extractor(self):
        return RawMaterialPriceExtractor(date=self.date)

    def bridge(self):
        """
            bridge btw data_pipeline & feature_extractor
        """
        data_pipeline = OpenDataRawMaterialPrice(date=self.date)

        if data_pipeline.process():
            # TODO: handle exit code is 1 (fail)
            sys.exit()
        else:
            return self.feature_extractor.process()

    def process(self, data_process=False):
        """
        :param data_process:
            True: force to do data pipeline.
            False: directly do feature extraction from process data.
        :return: df, key
        """
        if data_process is True:
            self.logger.info("force to do data pipeline for raw material price")
            return self.bridge()
        else:
            try:
                return self.feature_extractor.process()
            except Exception as e:
                self.logger.info("there is no raw material price data in process bucket")
                return self.bridge()


def main():
    fp = RawMaterialPriceExtractionPipeline(date="201905")
    df, key = fp.process()
    print(df)


if __name__ == '__main__':
    main()
