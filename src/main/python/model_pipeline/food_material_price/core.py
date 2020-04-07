import pandas as pd
import numpy as np

from feature_extraction_pipeline.open_data_marine_weather.main import MarineWeatherExtractionPipeline
from feature_extraction_pipeline.open_data_raw_material_price.main import RawMaterialPriceExtractionPipeline
from feature_extraction_pipeline.open_data_terrestrial_weather.main import TerrestrialWeatherExtractionPipeline
from model.elastic_net import ElasticNetSearcher
from model.linear_regression import LinearRegressionModel
from util.build_dataset import build_process_fmp
from util.logging import init_logger


class PricePredictModelPipeline:

    def __init__(self, bucket_name: str):
        self.logger = init_logger()

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
        return df.drop(columns=["price", "date"]), df["price"]

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
        reversed_time = df["date"].drop_duplicates().sort_values(ascending=False).tolist()
        standard_date = reversed_time[predict_days]

        train = df[df["date"].dt.date < standard_date]
        test = df[df["date"].dt.date >= standard_date]
        return train, test

    def process(self, date: str, data_process: bool):
        """
        :return: exit code
        """
        try:
            # build dataset
            dataset = build_process_fmp(date=date, process=data_process)

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
            self.logger.info("coef:\n{coef}".format(
                coef=pd.Series(searcher.searcher.best_estimator_.coef_, index=train_x.columns)
            ))
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


