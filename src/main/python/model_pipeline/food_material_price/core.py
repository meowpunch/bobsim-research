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

        # s3
        self.bucket_name = bucket_name

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
        # TODO: it should be processed in data_pipeline
        reversed_time = df["조사일자"].drop_duplicates().sort_values(ascending=False).tolist()
        standard_date = reversed_time[predict_days]

        train = df[df.조사일자.dt.date < standard_date]
        test = df[df.조사일자.dt.date >= standard_date]
        return train, test

    def process(self):
        """
        :return: exit code
        """
        try:
            # build dataset
            dataset = self.build_dataset()

            # set train, test dataset
            train, test = self.set_train_test(dataset)
            train_x, train_y = self.split_xy(train)
            test_x, test_y = self.split_xy(test)

            # hyperparameter tuning
            searcher = ElasticNetSearcher(
                x_train=train_x, y_train=train_y, score=self.customized_rmse
            )
            searcher.fit()
            self.logger.info("tuned params are {params}".format(params=searcher.get_best_params()))

            # through inverse function, get metric (customized rmse)
            pred_y = searcher.predict(test_x)
            score = self.customized_rmse(test_y, pred_y)
            self.logger.info("customized RMSE is {score}".format(score=score))

            # save model
            searcher.save(
                bucket_name=self.bucket_name,
                key="food_material_price_predict_model/model.pkl"
            )
        except Exception as e:
            # TODO: consider that it can repeat to save one more time
            self.logger.critical(e, exc_info=True)
            return 1

    def build_price(self):
        """
        :return: price DataFrame and key
        """
        # extract features
        price, key = RawMaterialPriceExtractionPipeline(date=self.date).process()
        return price, key

    def build_weather(self):
        """
        :return: weather DataFrame and key
        """
        # extract weather features
        t_weather, t_key = TerrestrialWeatherExtractionPipeline(date=self.date).process()
        m_weather, m_key = MarineWeatherExtractionPipeline(date=self.date).process()

        # combine marine and terrestrial weather
        weather = pd.merge(
            t_weather, m_weather,
            how='inner', left_on=t_key, right_on=m_key
        )
        return weather, t_key

    def build_dataset(self):
        """
            after build price and weather, join them
        :return: combined pd DataFrame
        """
        price, p_key = self.build_price()
        weather, w_key = self.build_weather()
        return pd.merge(
            price, weather, how="inner", left_on=p_key, right_on=w_key
        ).drop("일시", axis=1).astype(dtype={"조사일자": "datetime64"})
