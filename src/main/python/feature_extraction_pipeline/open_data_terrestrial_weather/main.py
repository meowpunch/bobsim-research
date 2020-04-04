import sys

from data_pipeline.open_data_terrestrial_weather.core import OpenDataTerrestrialWeather
from feature_extraction_pipeline.open_data_terrestrial_weather.core import TerrestrialWeatherExtractor
from util.logging import init_logger


class TerrestrialWeatherExtractionPipeline:
    def __init__(self, date: str):
        self.date = date

        self.logger = init_logger()

    @property
    def feature_extractor(self):
        return TerrestrialWeatherExtractor(date=self.date)

    def process(self):
        try:
            return self.feature_extractor.process()
        except Exception as e:
            self.logger.info("there is no data in process bucket")
            data_pipeline = OpenDataTerrestrialWeather(date=self.date)

            if data_pipeline.process():
                # TODO: handle exit code is 1 (fail)
                sys.exit()
            else:
                return self.feature_extractor.process()


def main():
    fp = TerrestrialWeatherExtractionPipeline(date="201905")
    df, key = fp.process()
    print(df)


if __name__ == '__main__':
    main()
