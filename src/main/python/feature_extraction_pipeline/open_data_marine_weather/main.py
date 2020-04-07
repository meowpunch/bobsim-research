import sys

from data_pipeline.open_data_marine_weather.core import OpenDataMarineWeather
from feature_extraction_pipeline.open_data_marine_weather.core import MarineWeatherExtractor
from util.logging import init_logger


class MarineWeatherExtractionPipeline:
    def __init__(self, date: str):
        self.date = date

        self.logger = init_logger()

    @property
    def feature_extractor(self):
        return MarineWeatherExtractor(date=self.date)

    def bridge(self):
        """
            bridge btw data_pipeline & feature_extractor
        """
        data_pipeline = OpenDataMarineWeather(date=self.date)

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
            self.logger.info("force to do data pipeline for marine weather")
            return self.bridge()
        else:
            try:
                return self.feature_extractor.process()
            except Exception as e:
                self.logger.info("there is no marine weather data in process bucket")
                return self.bridge()


def main():
    fp = MarineWeatherExtractionPipeline(date="201905")
    df, key = fp.process()
    print(df)


if __name__ == '__main__':
    main()
