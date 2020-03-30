import pandas as pd
import numpy as np

from feature_extraction_pipeline.open_data_marine_weather.main import MarineWeatherExtractionPipeline
from feature_extraction_pipeline.open_data_raw_material_price.main import RawMaterialPriceExtractionPipeline
from feature_extraction_pipeline.open_data_terrestrial_weather.main import TerrestrialWeatherExtractionPipeline
from model.elastic_net import ElasticNetSearcher
from model.linear_regression import LinearRegressionModel
from util.logging import init_logger


class PricePredictModelPipeline:

    def __init__(self, bucket_name: str, date: str):
        self.logger = init_logger()
        self.date = date

        self.bucket_name = bucket_name

        # extract feature
        price, p_key = RawMaterialPriceExtractionPipeline(date=self.date).process()
        t_weather, t_key = TerrestrialWeatherExtractionPipeline(date=self.date).process()
        m_weather, m_key = MarineWeatherExtractionPipeline(date=self.date).process()

        # combine data
        weather = pd.merge(
            t_weather.groupby(["일시"]).mean(),
            m_weather.groupby(["일시"]).mean(),
            how='inner', left_on=t_key, right_on=m_key
        ).reset_index()

        self.dataset = pd.merge(
            price, weather,
            how="left", left_on=p_key, right_on=t_key
        ).drop("일시", axis=1).astype(dtype={"조사일자": "datetime64"})

    @staticmethod
    def customized_rmse(y, y_pred):
        error = y - y_pred

        def penalize(x):
            if x > 0:
                # if y > y_pred, penalize 10%
                return x * 1.1
            else:
                return x
        X = np.vectorize(penalize)(error)
        return np.sqrt(np.square(X).mean())

    @staticmethod
    def split_xy(df: pd.DataFrame):
        return df.drop(columns=["당일조사가격", "조사일자"]), df["당일조사가격"]

    def get_score(self, train, test):
        x_train, y_train = self.split_xy(train)
        x_test, y_test = self.split_xy(test)

        regr = LinearRegressionModel(x_train, y_train)
        regr.fit()
        y_pred = regr.predict(x_test)
        return self.customized_rmse(np.expm1(y_test), np.expm1(y_pred))

    def set_train_test(self, df: pd.DataFrame):
        """
            TODO: search grid to find proper train test volume
        :param df: dataset
        :return: train Xy, test Xy
        """
        predict_days = 7
        reversed_time = df["조사일자"].drop_duplicates().sort_values(ascending=False).tolist()
        standard_date = reversed_time[predict_days]

        train = df[df.조사일자.dt.date < standard_date]
        test = df[df.조사일자.dt.date >= standard_date]
        return train, test

    def process(self):
        """
            TODO: consider Step 1
            1. set train, test volume (LinearRegression)
            2. hyperparameter tuning (ElasticNet)
            3. save model
        :return: exit code
        """
        # set train, test dataset
        train, test = self.set_train_test(self.dataset)
        train_x, train_y = self.split_xy(train)
        test_x, test_y = self.split_xy(test)

        # hyperparameter tuning
        searcher = ElasticNetSearcher(
            x_train=train_x, y_train=train_y, score=self.customized_rmse
        )
        searcher.fit()

        # log score
        pred_y = searcher.predict(test_x)
        score = self.customized_rmse(np.expm1(test_y), np.expm1(pred_y))
        self.logger.info("customized RMSE is {score}".format(score=score))

        # save model
        searcher.save(
            bucket_name=self.bucket_name,
            key="food_material_price_predict_model/model.pkl"
        )
