import pandas as pd
from sklearn.metrics import mean_squared_error

from feature_extraction_pipeline.open_data_marine_weather.main import MarineWeatherExtractionPipeline
from feature_extraction_pipeline.open_data_raw_material_price.main import RawMaterialPriceExtractionPipeline
from feature_extraction_pipeline.open_data_terrestrial_weather.main import TerrestrialWeatherExtractionPipeline
from util.logging import init_logger


class PricePredictModelPipeline:

    def __init__(self, date: str):
        self.logger = init_logger()
        self.date = date

        price, p_key = RawMaterialPriceExtractionPipeline(date=self.date).process()
        t_weather, t_key = TerrestrialWeatherExtractionPipeline(date=self.date).process()
        m_weather, m_key = MarineWeatherExtractionPipeline(date=self.date).process()

        weather = pd.merge(
            t_weather.groupby(["일시"]).mean(),
            m_weather.groupby(["일시"]).mean(),
            how='inner', left_on=t_key, right_on=m_key
        ).reset_index()

        self.dataset = pd.merge(
            price, weather,
            how="left", left_on=p_key, right_on=t_key
        ).drop("일시", axis=1).astype(dtype={"조사일자": "datetime64"})

        print(self.dataset)

    def join(self, ):
        [ass]

    def process(self):
        """
            TODO: logic comes here
        :return: undefined
        """
        # extract features

