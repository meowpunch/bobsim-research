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

    def process(self):
        try:
            return self.feature_extractor.process()
        except Exception as e:
            self.logger.info("there is no data in process bucket")
            data_pipeline = OpenDataRawMaterialPrice(date=self.date)

            if data_pipeline.process():
                # TODO: handle exit code is 1 (fail)
                sys.exit()
            else:
                return self.feature_extractor.process()


def main():
    fp = RawMaterialPriceExtractionPipeline(date="201905")
    df, key = fp.process()
    print(df)


if __name__ == '__main__':
    main()
