import pandas as pd
from scipy.stats import truncnorm
import numpy as np

from util.visualize import plot


def get_truncated_normal(mean=0, sd=1, low=0, upp=10):
    return truncnorm(
        (low - mean) / sd, (upp - mean) / sd, loc=mean, scale=sd)


def analyze(num, exist, avg, delta, d_type=None):
    """
        TODO: upgrade 'exist' -> 'quantity'

    :param num:
    :param exist:
    :param avg:
    :param delta:
    :param d_type:
    :return:
    """
    mean, sigma = float(avg), delta*0.5
    # x_price = np.array([(0 if q is 0 else ) for q in x_quantity])

    x = get_truncated_normal(mean=int(mean), sd=sigma, low=mean - delta, upp=mean + delta)
    x = x.rvs(num)

    """
        In Korean, there is a currency from 10 digits.
        so round(, -1)
    """
    x_rounded = np.round(x.astype(int), -1)

    # for visualize
    # plot(data=[x])gi

    return x_rounded*exist


# core function
def price(data, num=1):
    """
        TODO:
            1. generate data from statistic (analyze)
            2. filter
    """
    p_data = data.apply(lambda x: pd.Series({'price': analyze(
        num=num,
        exist=x.quantity,
        avg=x.average,
        delta=x.delta,
        d_type=x.distr_type
    )}), axis=1)
    # for checking

    # attr: id, name, quantity, price
    return pd.concat([data[['id', 'name', 'sensitivity', 'quantity']], p_data], axis=1)


