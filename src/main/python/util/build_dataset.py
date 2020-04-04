import pandas as pd

from data_pipeline.open_data_marine_weather.core import OpenDataMarineWeather
from data_pipeline.open_data_raw_material_price.core import OpenDataRawMaterialPrice
from data_pipeline.open_data_terrestrial_weather.core import OpenDataTerrestrialWeather
from feature_extraction_pipeline.open_data_marine_weather.main import MarineWeatherExtractionPipeline
from feature_extraction_pipeline.open_data_raw_material_price.main import RawMaterialPriceExtractionPipeline
from feature_extraction_pipeline.open_data_terrestrial_weather.main import TerrestrialWeatherExtractionPipeline


# TODO: consider of classification

def build_origin_fmp(date, prefix):
    # load origin data filtered
    p_df, p_key = build_origin_price(date=date, prefix=prefix)
    w_df, w_key = build_origin_weather(date=date, prefix=prefix)

    # merge
    return pd.merge(
        p_df, w_df, how="inner", left_on=p_key, right_on=w_key
    ).drop("일시", axis=1).astype(dtype={"조사일자": "datetime64"})


def build_origin_price(date, prefix):
    p = OpenDataRawMaterialPrice(date=date)
    filtered = p.filter(p.input_df)
    if prefix == "filter":
        return filtered, "조사일자"
    else:
        return p.clean(filtered), "조사일자"


def build_origin_weather(date, prefix):
    t = OpenDataTerrestrialWeather(date=date)
    t_df = t.filter(t.input_df)

    m = OpenDataMarineWeather(date=date)
    m_df = m.filter(m.input_df)

    if prefix == "filter":
        return pd.merge(
            t_df, m_df, how='inner', on="일시"
        ), "일시"
    else:
        return pd.merge(
            t.clean(t_df), m.clean(m_df), how='inner', on="일시"
        ), "일시"


def build_process_fmp(date):
    """
        after build price and weather, join them
    :return: combined pd DataFrame
    """
    price, p_key = build_process_price(date=date)
    weather, w_key = build_process_weather(date=date)
    return pd.merge(
        price, weather, how="inner", left_on=p_key, right_on=w_key
    ).drop("일시", axis=1).astype(dtype={"조사일자": "datetime64"})


def build_process_price(date):
    """
    :return: price DataFrame and key
    """
    # extract features
    price, key = RawMaterialPriceExtractionPipeline(date=date).process()
    return price, key


def build_process_weather(date):
    """
    :return: weather DataFrame and key
    """
    # extract weather features
    t_weather, t_key = TerrestrialWeatherExtractionPipeline(date=date).process()
    m_weather, m_key = MarineWeatherExtractionPipeline(date=date).process()

    # combine marine and terrestrial weather
    weather = pd.merge(
        t_weather, m_weather,
        how='inner', left_on=t_key, right_on=m_key
    )
    return weather, t_key


# core
def build_master(dataset="origin_fmp", date="201908"):
    """
    :param dataset:
        - food material price predict model
        'origin_fmp': origin
        'process_fmp': process

    :param date: str represents date
    :return: pd DataFrame combined
    """
    if dataset == "clean_origin_fmp":
        return build_origin_fmp(date=date, prefix="clean")
    elif dataset == "filter_origin_fmp":
        return build_origin_fmp(date=date, prefix="filter")
    elif dataset == "process_fmp":
        return build_process_fmp(date=date)
    else:
        raise Exception("not supported")


def main():
    """
        test for build master
    """
    # origin_df = build_master("origin_fmp", date="201908")
    # print(origin_df)
    #
    # process_df = build_master("process_fmp", date="201908")
    # print(process_df)

    # p_df, p_key = build_process_price(date="201908")
    # print(p_df)
    # print(p_df.info())

    filter_origin_df = build_master(dataset="filter_origin_fmp", date="201908")
    clean_origin_df = build_master(dataset="clean_origin_fmp", date="201908")

    print(filter_origin_df.info())
    print(clean_origin_df.info())


if __name__ == '__main__':
    main()