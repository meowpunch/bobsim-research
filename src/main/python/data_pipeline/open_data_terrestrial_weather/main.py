from data_pipeline.open_data_terrestrial_weather.core import OpenDataTerrestrialWeather


def main():
    open_data_terrestrial_weather = OpenDataTerrestrialWeather()
    open_data_terrestrial_weather.process()


if __name__ == '__main__':
    main()
