from data_pipeline.open_data_marine_weather.core import OpenDataMarineWeather


def main():
    open_data_marine_weather = OpenDataMarineWeather()
    open_data_marine_weather.process()


if __name__ == '__main__':
    main()
