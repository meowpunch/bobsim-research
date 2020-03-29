from data_pipeline.open_data_marine_weather.core import OpenDataMarineWeather


def main():
    open_data_marine_weather = OpenDataMarineWeather(
        date="201908"
    )
    open_data_marine_weather.process()


if __name__ == '__main__':
    main()
