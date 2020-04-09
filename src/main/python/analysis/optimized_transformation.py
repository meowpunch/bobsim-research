import pandas as pd
import functools
from sklearn.preprocessing import QuantileTransformer, PowerTransformer, MinMaxScaler, StandardScaler, RobustScaler, \
    MaxAbsScaler, Normalizer
import numpy as np

from sklearn.base import BaseEstimator, TransformerMixin
from scipy.stats import skew

# transformer
from util.build_dataset import build_origin_fmp
from util.pandas import get_numeric_df


def log_transform(X):
    return np.log1p(X)


def sqrt_transform(X):
    return np.sqrt(X)


def get_skews(df):
    return df.apply(lambda x: skew(x))


def sum_corr(df):
    # default: method=pearson, min_periods=1
    # method{‘pearson’, ‘kendall’, ‘spearman’}
    corr = df.corr()
    return abs(corr['price'].drop('price')).sum()


def transform(transformer, df):
    if isinstance(transformer, TransformerMixin):
        return pd.DataFrame(transformer.fit_transform(df), columns=df.columns)
    elif transformer == 'None':
        return df
    else:
        return transformer(df)


def corr_xy(x, y):
    corr = pd.concat([x, y], axis=1).corr()
    return abs(corr['price']).drop('price').sum()


def greedy_search(column, X: pd.DataFrame, t_X: list, y: pd.Series):
    """
        iterate transformer for X and compare with y (corr_xy)
    """
    x = X[column]
    l_tx = list(map(functools.partial(transform, df=pd.DataFrame(x)), t_X))
    l_coef = list(map(functools.partial(corr_xy, y=y), l_tx))

    # find max coef and index
    max_coef = max(l_coef)
    max_index = l_coef.index(max_coef)

    proper_transformer = t_X[max_index]
    return proper_transformer


def iterate_x(y: pd.Series, X: pd.DataFrame, t_X: list):
    # iterate X
    return list(map(functools.partial(greedy_search, X=X, t_X=t_X, y=y), X.columns.tolist()))


def search_transformers(X: pd.DataFrame, y: pd.Series, transformers_X: list, transformers_y: list):
    """
    return: result grid, pd DataFrame
    """
    l_ty = list(map(functools.partial(transform, df=pd.DataFrame(y)), transformers_y))

    # iterate y
    return list(map(functools.partial(iterate_x, X=X, t_X=transformers_X), l_ty))
    # return pd.DataFrame(result, columns=X.columns, index=t_names_y)


def optimized_transform(X: pd.DataFrame, y: pd.Series, transformers_X: list, transformers_y: list):
    """
        optimized transformation of X for transformed y
    :return: dict -> { transformer_y: [X_optimized_transformers] }
    """
    # have to make numeric df
    numeric_X = get_numeric_df(X)
    return dict(zip(transformers_y, search_transformers(numeric_X, y, transformers_X, transformers_y)))


def main():
    # log = log_transform
    # sqrt = sqrt_transform
    # standard = StandardScaler()
    #
    # t_names_X = ['log', 'None']
    # t_names_y = ['log', 'standard', 'None']
    # transformers_X = [log, 'None']
    # transformers_y = [log, standard, 'None']
    #
    # origin_df = build_origin_fmp(bucket_name="production-bobsim", date="201908", prefix='clean')
    # res = optimized_transform(
    #     X=origin_df.drop(columns="price", axis=1), y=origin_df["price"],
    #     transformers_X=transformers_X, transformers_y=transformers_y
    # )
    # print(res)
    pass


if __name__ == '__main__':
    main()
