import sys

from data_pipeline.open_data_raw_material_price.core import OpenDataRawMaterialPrice
from feature_extraction_pipeline.open_data_raw_material_price.core import RawMaterialPriceExtractor


def main():
    data_pipeline = OpenDataRawMaterialPrice(
        date="201908"
    )

    if data_pipeline.process():
        # TODO: handle exit code is 1 (fail)
        sys.exit()
    else:
        feature_extractor = RawMaterialPriceExtractor()
        feature_extractor.process()


if __name__ == '__main__':
    main()
