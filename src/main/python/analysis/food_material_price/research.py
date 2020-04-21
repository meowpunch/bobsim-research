from analysis.hit_ratio_error import hit_ratio_error
from model.elastic_net import ElasticNetModel, ElasticNetSearcher
import numpy as np
import pandas as pd

from util.s3_manager.manage import S3Manager
from util.visualize import draw_hist


def search_process(dataset, bucket_name, date, term):
    """
        ElasticNetSearcher for research
    :return: exit code
    """
    train_x, train_y, test_x, test_y = dataset

    # hyperparameter tuning
    searcher = ElasticNetSearcher(
        x_train=train_x, y_train=train_y, bucket_name=bucket_name, score=customized_rmse,
        grid_params={
            "max_iter": [1, 5, 10],
            "alpha": [0, 0.00001, 0.0001, 0.001, 0.01, 0.1, 1, 10, 100],
            "l1_ratio": np.arange(0.0, 1.0, 0.1)
        })
    searcher.fit(train_x, train_y)

    # predict & metric
    pred_y = searcher.predict(X=test_x)
    # r_test, r_pred = inverse_price(test_y), inverse_price(pred_y)
    metric = searcher.estimate_metric(y=test_y, predictions=pred_y)

    # save
    # TODO self.now -> date set term, e.g. 010420 - 120420
    searcher.save_params(key="food_material_price_predict_model/model/research/params.pkl")
    searcher.save_metric(key="food_material_price_predict_model/model/research/metric.pkl")
    searcher.save_coef(key="food_material_price_predict_model/model/research/beta.csv")
    return metric


def inverse_price(self, price):
    manager = S3Manager(bucket_name=self.bucket_name)
    mean, std = manager.load_dump(
        key="food_material_price_predict_model/price_(mean,std)_{date}.pkl".format(date=self.date)
    )
    return price * std + mean


def error_distribution(self, y, y_pred):
    manager = S3Manager(bucket_name=self.bucket_name)
    err = pd.Series(y - y_pred).rename("error")
    draw_hist(err)
    manager.save_plt_to_png(
        key="food_material_price_predict_model/image/error_distribution_{date}.png".format(date=self.date)
    )

    ratio = hit_ratio_error(err)
    manager.save_plt_to_png(
        key="food_material_price_predict_model/image/hit_ratio_error_{date}.png".format(date=self.date)
    )
    return ratio


def split_xy(df: pd.DataFrame):
    return df.drop(columns=["price", "date"]), df["price"]


def customized_rmse(y, y_pred):
    errors = y - y_pred

    def penalize(err):
        # if y > y_pred, penalize 10%
        return err * 2 if err > 0 else err

    return np.sqrt(np.square(np.vectorize(penalize)(errors)).mean())


def untuned_process(dataset):
    train_x, train_y, test_x, test_y = dataset

    # hyperparameter tuning
    model = ElasticNetModel(x_train=train_x, y_train=train_y)
    model.fit()
    self.beta(model)
    model.save_coef(bucket_name=self.bucket_name, key="food_material_price_predict_model/linear_coef.csv")

    # analyze metric and coef(beta)
    pred_y = model.predict(X=test_x)
    return self.metric(test_y, pred_y)
