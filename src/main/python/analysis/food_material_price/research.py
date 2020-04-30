import datetime
import functools
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.linear_model import ElasticNet

from model.elastic_net import ElasticNetModel, ElasticNetSearcher
from util.s3_manager.manage import S3Manager
from util.visualize import series_plot

"""
    Grid search (hyperparameter tuning)
    search for tuned parameters, best model, best coef, metric, error distribution
"""


def search_process(dataset, bucket_name, term, grid_params):
    """
        ElasticNetSearcher for research
    :param dataset: merged 3 dataset (raw material price, terrestrial weather, marine weather)
    :param bucket_name: s3 bucket name
    :param term: term of researched dataset
    :param grid_params: grid for searching best parameters
    :return: metric (customized rmse)
    """
    train_x, train_y, test_x, test_y = dataset

    # hyperparameter tuning
    searcher = ElasticNetSearcher(
        x_train=train_x, y_train=train_y, bucket_name=bucket_name,
        score=customized_rmse, grid_params=grid_params
    )
    searcher.fit(train_x, train_y)

    # predict & metric
    pred_y = searcher.predict(X=test_x)
    # r_test, r_pred = inverse_price(test_y), inverse_price(pred_y)
    metric = searcher.estimate_metric(y_true=test_y, y_pred=pred_y)

    # save
    # TODO self.now -> date set term, e.g. 010420 - 120420
    searcher.save(prefix="food_material_price_predict_model/research/{date}".format(date=term))
    searcher.save_params(key="food_material_price_predict_model/research/tuned_params.pkl")
    return metric


def untuned_process(dataset, bucket_name):
    """
        untuned ElasticNet(linear) for research
    :param bucket_name: s3 bucket name
    :param dataset: merged 3 dataset (raw material price, terrestrial weather, marine weather)
    :return: metric
    """
    train_x, train_y, test_x, test_y = dataset

    # hyperparameter tuning
    model = ElasticNetModel(
        bucket_name=bucket_name,
        x_train=train_x, y_train=train_y
    )
    model.fit()

    # predict & metric
    pred_y = model.predict(X=test_x)
    # r_test, r_pred = inverse_price(test_y), inverse_price(pred_y)
    metric = model.estimate_metric(scorer=customized_rmse, y=test_y, predictions=pred_y)

    # save
    model.save(prefix="food_material_price_predict_model/research/linear")
    return metric


"""
    Metric by test size or train size
    find train test volume for timeseries dataset 
"""


def metric_by_test_size(df: pd.DataFrame, test_sizes=range(1, 10), train_size: int = 5,
                        date=datetime.datetime.now().strftime("%m%Y")):
    """
            fix train size and adjust test size
    :param df: pd.DataFrame
    :param test_sizes: list of test sizes
    :param train_size: 5 -> 5 days (one week, no weekend)
    :param date: research date
    """
    # split dataset to train, test
    time_series = df["date"].drop_duplicates().tolist()
    train_test = map(
        lambda test_size: split_time_series(
            str_d=time_series[0], mid_d=time_series[train_size], end_d=time_series[train_size + test_size], df=df),
        test_sizes)

    # scoring
    param = S3Manager(bucket_name="production-bobsim").load_dump(
        key="food_material_price_predict_model/research/tuned_params.pkl")
    scores = map(lambda x: scoring(x, param=param), train_test)
    ser = pd.Series(scores, index=test_sizes)

    # plot
    series_plot(ser=ser, kind="bar", x_label="test size", y_label="customized RMSE",
                title="train_size: {}".format(train_size))

    # save
    S3Manager(bucket_name="production-bobsim").save_plt_to_png(
        key="food_material_price_predict_model/research/{date}/image/metric_by_test_size_train{train}.png".format(
            date=date, train=train_size))


def metric_by_train_size(df: pd.DataFrame, train_sizes=range(1, 10), test_size: int = 5,
                         date=datetime.datetime.now().strftime("%m%Y")):
    """
                fix test size and adjust train size (not used)
        :param df: pd.DataFrame
        :param train_sizes: list of train sizes
        :param test_size: 5 -> 5 days (one week, no weekend)
        :param date: research date
    """
    # split dataset to train, test
    time_series = df["date"].drop_duplicates().tolist()
    train_test = map(
        lambda train_size: split_time_series(
            str_d=time_series[-1 * (test_size + train_size)], mid_d=time_series[-1 * test_size], end_d=time_series[-1],
            df=df),
        train_sizes)

    # scoring
    param = S3Manager(bucket_name="production-bobsim").load_dump(
        key="food_material_price_predict_model/research/tuned_params.pkl")
    scores = map(lambda x: scoring(x, param=param), train_test)
    ser = pd.Series(scores, index=train_sizes)

    # plot
    series_plot(ser=ser, kind="bar", x_label="train size", y_label="customized RMSE",
                title="test_size: {}".format(test_size))

    # save
    S3Manager(bucket_name="production-bobsim").save_plt_to_png(
        key="food_material_price_predict_model/research/{date}/image/metric_by_train_size_test{test}.png".format(
            date=date, test=test_size))


"""
    Metric by other term
"""


def metric_by_other_term(df: pd.DataFrame, train_size: int, test_size: int, n_days=range(10),
                         date=datetime.datetime.now().strftime("%m%Y")):
    """
        measure the metric by pushing the term of train/test data set by day
    :param df: dataset
    :param train_size: term of train data
    :param test_size:  term of test data
    :param n_days: how many days do you push aside.
    :param date: research date
    """
    # split dataset to train, test
    time_series = df["date"].drop_duplicates().tolist()
    train_test = map(
        lambda x: split_time_series(
            str_d=time_series[x], mid_d=time_series[x + train_size], end_d=time_series[x + train_size + test_size],
            df=df),
        n_days)

    # scoring
    param = S3Manager(bucket_name="production-bobsim").load_dump(
        key="food_material_price_predict_model/research/tuned_params.pkl")
    scores = map(lambda x: scoring(x, param=param), train_test)
    ser = pd.Series(scores, index=n_days)

    # plot
    series_plot(ser=ser, kind="bar", x_label="day", y_label="customized RMSE",
                title="train, test size: {}, {}".format(train_size, test_size), d=0.3)

    # save
    S3Manager(bucket_name="production-bobsim").save_plt_to_png(
        key="food_material_price_predict_model/research/{date}/image/metric_by_other_term_train{train}/test{test}.png".format(
            date=date, train=train_size, test=test_size))


"""
    General
"""


def scoring(train_test: tuple, param=None):
    # split x, y
    train, test = train_test
    x_train, y_train = split_xy(train)
    x_test, y_test = split_xy(test)

    # fit & predict
    regr = ElasticNet(**param)
    regr.fit(x_train, y_train)
    y_pred = regr.predict(x_test)

    # metric
    return customized_rmse(y_test, y_pred)


def split_time_series(str_d, mid_d, end_d, df):
    train = df[(str_d <= df["date"].dt.date) & (df["date"].dt.date <= mid_d)]
    test = df[(mid_d < df["date"].dt.date) & (df["date"].dt.date <= end_d)]
    return train, test


def customized_rmse(y_true, y_pred):
    # TODO: apply this to objective function of model
    errors = y_true - y_pred

    def penalize(err):
        # if y > y_pred, penalize 10%
        return err * 2 if err > 0 else err

    return np.sqrt(np.square(np.vectorize(penalize)(errors)).mean())


def split_xy(df: pd.DataFrame):
    return df.drop(columns=["price", "date"]), df["price"]


def inverse_price(self, price):
    manager = S3Manager(bucket_name=self.bucket_name)
    mean, std = manager.load_dump(
        key="food_material_price_predict_model/price_(mean,std)_{date}.pkl".format(date=self.date)
    )
    return price * std + mean
