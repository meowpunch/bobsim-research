import datetime
import functools
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.linear_model import ElasticNet

from model.elastic_net import ElasticNetModel, ElasticNetSearcher
from util.s3_manager.manage import S3Manager

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


"""
    Metric by test size
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
    # scoring
    score_list = list(map(functools.partial(scoring, train_size=train_size, df=df), test_sizes))
    ser = pd.Series(score_list, index=test_sizes)

    # plot
    series_plot(ser=ser, kind="bar", x_label="test size", y_label="customized RMSE",
                title="train_size: {}".format(train_size))

    # save
    S3Manager(bucket_name="production-bobsim").save_plt_to_png(
        key="food_material_price_predict_model/research/{date}/image/rmse_fixing_train_size_{train}.png".format(
            date=date, train=train_size))


def scoring(test_size, train_size, df):
    x_train, y_train, x_test, y_test = split_dataset(test_size, train_size=train_size, df=df)

    # load tuned param
    params = S3Manager(bucket_name="production-bobsim").load_dump(
        key="food_material_price_predict_model/research/tuned_params.pkl")

    # fit & predict
    regr = ElasticNet(**params)
    regr.fit(x_train, y_train)
    y_pred = regr.predict(x_test)

    # metric
    return customized_rmse(y_test, y_pred)


def split_dataset(test_size, train_size, df):
    # split train/test & x/y
    time_series = df["date"].drop_duplicates().tolist()
    train, test = split_train_test(
        str_d=time_series[0], mid_d=time_series[train_size], end_d=time_series[test_size + train_size], df=df
    )
    x_train, y_train = split_xy(train)
    x_test, y_test = split_xy(test)
    return x_train, y_train, x_test, y_test


def split_train_test(str_d, mid_d, end_d, df: pd.DataFrame):
    """
        str [- train -] std (- test -] end
    :return: train, test dataset
    """
    train = df[(str_d <= df["date"].dt.date) & (df["date"].dt.date <= mid_d)]
    test = df[(mid_d < df["date"].dt.date) & (df["date"].dt.date <= end_d)]
    return train, test


def metric_by_train_size(df: pd.DataFrame, train_sizes=range(1, 10), test_size: int = 5,
                         date=datetime.datetime.now().strftime("%m%Y")):
    """
                fix test size and adjust train size (not used)
        :param df: pd.DataFrame
        :param train_sizes: list of train sizes
        :param test_size: 5 -> 5 days (one week, no weekend)
        :param date: research date
    """
    # scoring
    score_list = list(map(functools.partial(scoring2, test_size=test_size, df=df), train_sizes))
    ser = pd.Series(score_list, index=train_sizes)

    # plot
    series_plot(ser=ser, kind="bar", x_label="train size", y_label="customized RMSE",
                title="test_size: {}".format(test_size))

    # save
    S3Manager(bucket_name="production-bobsim").save_plt_to_png(
        key="food_material_price_predict_model/research/{date}/image/rmse_fixing_test_size_{test}.png".format(
            date=date, test=test_size))


def scoring2(train_size, test_size, df):
    time_series = df["date"].drop_duplicates().tolist()
    train, test = split_train_test(str_d=time_series[-1 * (test_size + train_size)], mid_d=time_series[-1 * test_size],
                                   end_d=time_series[-1], df=df)

    x_train, y_train = split_xy(train)
    x_test, y_test = split_xy(test)

    params = {'alpha': 0.0001, 'l1_ratio': 0.9, 'max_iter': 5}
    regr = ElasticNet(**params)
    regr.fit(x_train, y_train)
    y_pred = regr.predict(x_test)

    metric = customized_rmse(y_test, y_pred)
    return metric


def series_plot(ser: pd.Series, kind="bar", y_label="y", x_label="x", title="Untitled"):
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    ser_max, ser_min = ser.max(), ser.min()
    delta = (ser_max - ser_min) * 0.1
    ser.plot(kind=kind, ylim=(ser_min - delta, ser_max + delta), title=title)


"""
    Metric by other day
"""


def metric_by_other_day(df: pd.DataFrame, train_size: int, test_size: int):
    pass
