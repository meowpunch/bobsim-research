import pandas as pd

from data_pipeline.open_data_marine_weather.core import OpenDataMarineWeather
from data_pipeline.open_data_raw_material_price.core import OpenDataRawMaterialPrice
from data_pipeline.open_data_terrestrial_weather.core import OpenDataTerrestrialWeather
from feature_extraction_pipeline.open_data_marine_weather.main import MarineWeatherExtractionPipeline
from feature_extraction_pipeline.open_data_raw_material_price.main import RawMaterialPriceExtractionPipeline
from feature_extraction_pipeline.open_data_terrestrial_weather.main import TerrestrialWeatherExtractionPipeline


# TODO: consider of classification

def build_origin_fmp(date, prefix=None):
    # load origin data filtered
    p_df, p_key = build_origin_price(date=date, prefix=prefix)
    w_df, w_key = build_origin_weather(date=date, prefix=prefix)

    if prefix is "clean":
        # merge
        return pd.merge(
            p_df, w_df, how="inner", left_on=p_key, right_on=w_key
        ).astype(dtype={"date": "datetime64"})
    else:
        return p_df, w_df


def build_origin_price(date, prefix=None):
    p = OpenDataRawMaterialPrice(date=date)
    if prefix is "clean":
        return p.clean(p.filter(p.input_df)), "date"
    else:
        return p.input_df, "date"


def build_origin_weather(date, prefix=None):
    t = OpenDataTerrestrialWeather(date=date)
    m = OpenDataMarineWeather(date=date)

    if prefix is "clean":
        return pd.merge(
            t.clean(t.input_df), m.clean(m.input_df), how='inner', on="date"
        ), "date"
    else:
        return pd.merge(
            t.input_df, m.input_df, how='inner', on="date"
        ), "date"


def build_process_fmp(date):
    """
        after build price and weather, join them
    :return: combined pd DataFrame
    """
    price, p_key = build_process_price(date=date)
    weather, w_key = build_process_weather(date=date)
    return pd.merge(
        price, weather, how="inner", left_on=p_key, right_on=w_key
    ).astype(dtype={"date": "datetime64"})


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
        # df combined with p_df, t_df, m_df
        return build_origin_fmp(date=date, prefix="clean")
    elif dataset == "origin_fmp":
        # p_df, w_df(t+m)
        return build_origin_fmp(date=date)
    elif dataset == "process_fmp":
        # df combined with p_df, t_df, m_df
        return build_process_fmp(date=date)
    else:
        raise Exception("not supported")


def main():
    """
        test for build master
    """
    pass
    # origin_df = build_master("origin_fmp", date="201908")
    # print(origin_df)
    #
    # process_df = build_master("process_fmp", date="201908")
    # print(process_df)

    # p_df, p_key = build_process_price(date="201908")
    # print(p_df)
    # print(p_df.info())

    # p_df, w_df = build_master(dataset="origin_fmp", date="201908")
    # clean_origin_df = build_master(dataset="clean_origin_fmp", date="201908")
    #
    # print(p_df.info())
    # print(w_df.info())
    # print(clean_origin_df.info())


if __name__ == '__main__':
    main()
