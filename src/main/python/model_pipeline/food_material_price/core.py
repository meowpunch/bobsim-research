from io import StringIO

import numpy as np
import pandas as pd

from analysis.hit_ratio_error import hit_ratio_error
from analysis.train_test_volume import set_train_test
from model.elastic_net import ElasticNetSearcher, ElasticNetModel
from model.linear_regression import LinearRegressionModel
from util.build_dataset import build_process_fmp, build_master
from util.logging import init_logger
from util.s3_manager.manage import S3Manager
from util.transform import load_from_s3
from util.visualize import draw_hist


class PricePredictModelPipeline:

    def __init__(self, bucket_name: str, logger_name: str, date: str):
        self.logger = init_logger()

        self.date = date

        # s3
        self.bucket_name = bucket_name

    @staticmethod
    def customized_rmse(y, y_pred):
        errors = y - y_pred

        def penalize(err):
            # if y > y_pred, penalize 10%
            out = err * 2 if err > 0 else err
            return out

        X = np.vectorize(penalize)(errors)
        return np.sqrt(np.square(X).mean())

    @staticmethod
    def split_xy(df: pd.DataFrame):
        return df.drop(columns=["price", "date"]), df["price"]

    def tuned_process(self, pipe_data: bool):
        # build dataset
        dataset = build_master(
            dataset="process_fmp", bucket_name=self.bucket_name,
            date=self.date, pipe_data=pipe_data
        )

        # set train, test dataset
        train, test = set_train_test(dataset)
        train_x, train_y = self.split_xy(train)
        test_x, test_y = self.split_xy(test)

        # hyperparameter tuning
        model = ElasticNetModel(
            x_train=train_x, y_train=train_y,
            params={'alpha': 0, 'l1_ratio': 0.0, 'max_iter': 10}  # {'alpha': 0.0001, 'l1_ratio': 0.9, 'max_iter': 5}
        )
        model.fit()
        self.beta(model)
        model.model.intercept_ = model.model.intercept_ + 300
        # analyze metric and coef(beta)
        pred_y = model.predict(X=test_x)
        # r_test, r_pred = self.inverse_price(test_y), self.inverse_price(pred_y)
        score = self.metric(test_y, pred_y)
        err = self.error_distribution(test_y, pred_y)

        return score, err

    def untuned_process(self, pipe_data: bool):
        # build dataset
        dataset = build_master(
            dataset="process_fmp", bucket_name=self.bucket_name,
            date=self.date, pipe_data=pipe_data
        )

        # set train, test dataset
        train, test = set_train_test(dataset)
        train_x, train_y = self.split_xy(train)
        test_x, test_y = self.split_xy(test)

        # hyperparameter tuning
        model = ElasticNetModel(x_train=train_x, y_train=train_y)
        model.fit()
        self.beta(model)
        model.save_coef(bucket_name=self.bucket_name, key="food_material_price_predict_model/linear_coef.csv")

        # analyze metric and coef(beta)
        pred_y = model.predict(X=test_x)
        return self.metric(test_y, pred_y)

    def search_process(self, pipe_data: bool):
        """
        :return: exit code
        """
        try:
            # build dataset
            dataset = build_master(
                dataset="process_fmp", bucket_name=self.bucket_name,
                date=self.date, pipe_data=pipe_data
            )

            # set train, test dataset
            train, test = set_train_test(dataset)
            train_x, train_y = self.split_xy(train)
            test_x, test_y = self.split_xy(test)

            # hyperparameter tuning
            searcher = ElasticNetSearcher(
                x_train=train_x, y_train=train_y, score=self.customized_rmse
            )
            searcher.fit()
            self.logger.info("tuned params are {params}".format(params=searcher.best_params_))

            # analyze metric and coef(beta)
            pred_y = searcher.predict(X=test_x)
            score = self.metric(test_y, pred_y)
            self.beta(searcher)
            self.error_distribution(test_y, pred_y)

            # save
            searcher.save_model(
                bucket_name=self.bucket_name,
                key="food_material_price_predict_model/model/model.pkl"
            )
            return score
        except Exception as e:
            # TODO: consider that it can repeat to save one more time
            self.logger.critical(e, exc_info=True)
            return 1

    def inverse_price(self, price):
        manager = S3Manager(bucket_name=self.bucket_name)
        mean, std = manager.load_dump(
            key="food_material_price_predict_model/price_(mean,std)_{date}.pkl".format(date=self.date)
        )
        return price * std + mean

    def error_distribution(self, y, y_pred):
        m = S3Manager(bucket_name=self.bucket_name)
        err = pd.Series(y - y_pred).rename("error")
        draw_hist(err)
        m.save_plt_to_png(
            key="food_material_price_predict_model/image/error_distribution_{date}.png".format(date=self.date)
        )

        ratio = hit_ratio_error(err)
        m.save_plt_to_png(
            key="food_material_price_predict_model/image/hit_ratio_error_{date}.png".format(date=self.date)
        )
        return ratio

    def metric(self, y, y_pred):
        score = self.customized_rmse(y, y_pred)
        self.logger.info("customized RMSE is {score}".format(score=score))
        return score

    def beta(self, model):
        self.logger.info("coef:\n{coef}".format(coef=model.coef_df))
        # save coef
        model.save_coef(bucket_name=self.bucket_name,
                        key="food_material_price_predict_model/beta_{date}.csv".format(date=self.date))
        return model.coef_df
