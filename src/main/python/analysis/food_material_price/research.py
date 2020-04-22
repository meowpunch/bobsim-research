import numpy as np
import pandas as pd

from model.elastic_net import ElasticNetModel, ElasticNetSearcher
from util.s3_manager.manage import S3Manager


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
