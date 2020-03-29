import sys

from data_pipeline.open_data_terrestrial_weather.core import OpenDataTerrestrialWeather
from feature_extraction_pipeline.open_data_terrestrial_weather.core import TerrestrialWeatherExtractor


class TerrestrialWeatherExtractionPipeline:
    def __init__(self, date: str):
        self.date = date

    def process(self):
        data_pipeline = OpenDataTerrestrialWeather(
            date=self.date
        )

        if data_pipeline.process():
            # TODO: handle exit code is 1 (fail)
            sys.exit()
        else:
            feature_extractor = TerrestrialWeatherExtractor(date=self.date)
            return feature_extractor.process()


def main():
    date="201908"
    data_pipeline = OpenDataTerrestrialWeather(
        date=date
    )

    if data_pipeline.process():
        # TODO: handle exit code is 1 (fail)
        sys.exit()
    else:
        feature_extractor = TerrestrialWeatherExtractor(date=date)
        feature_extractor.process()


if __name__ == '__main__':
    main()
