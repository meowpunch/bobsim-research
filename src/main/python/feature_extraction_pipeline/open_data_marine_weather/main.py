import sys

from data_pipeline.open_data_marine_weather.core import OpenDataMarineWeather
from feature_extraction_pipeline.open_data_marine_weather.core import MarineWeatherExtractor


class MarineWeatherExtractionPipeline:
    def __init__(self, date: str):
        self.date = date

    def process(self):
        data_pipeline = OpenDataMarineWeather(
            date=self.date
        )

        if data_pipeline.process():
            # TODO: handle exit code is 1 (fail)
            sys.exit()
        else:
            feature_extractor = MarineWeatherExtractor(date=self.date)
            return feature_extractor.process()


def main():
    date = "201908"
    data_pipeline = OpenDataMarineWeather(
        date=date
    )

    if data_pipeline.process():
        # TODO: handle exit code is 1 (fail)
        sys.exit()
    else:
        feature_extractor = MarineWeatherExtractor(date=date)
        return feature_extractor.process()


if __name__ == '__main__':
    main()
