import sys

from data_pipeline.open_data_raw_material_price.core import OpenDataRawMaterialPrice
from feature_extraction_pipeline.open_data_raw_material_price.core import RawMaterialPriceExtractor


class RawMaterialPriceExtractionPipeline:
    def __init__(self, date: str):
        # TODO: load data and pass to extractor
        self.date = date

    def process(self):
        data_pipeline = OpenDataRawMaterialPrice(
            date=self.date
        )

        if data_pipeline.process():
            # TODO: handle exit code is 1 (fail)
            sys.exit()
        else:
            feature_extractor = RawMaterialPriceExtractor(
                date=self.date
            )
            return feature_extractor.process()


def main():
    date = "201908"
    data_pipeline = OpenDataRawMaterialPrice(
        date=date
    )

    if data_pipeline.process():
        # TODO: handle exit code is 1 (fail)
        sys.exit()
    else:
        feature_extractor = RawMaterialPriceExtractor(date=date)
        feature_extractor.process()


if __name__ == '__main__':
    main()
