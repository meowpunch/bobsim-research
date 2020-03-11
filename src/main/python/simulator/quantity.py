"""
    first, just consider not the number but the existence of items.

    1. load item(food)
    2. make distribution of each item and randomly choose values
    3. binarize values by threshold
    4.
"""
import numpy as np
import pandas as pd
from scipy.stats import truncnorm

from util.visualize import plot


def mask_by_quantity(data, q_data):
    """
        TODO: more atomic about concat?
    """
    sub_dp = data.drop(["item_frequency"], axis=1)
    tmp_dp = pd.concat([sub_dp, q_data], axis=1)
    print(tmp_dp)

    # TODO: Which is better choice?
    # mask = (tmp_dp.quantity == True)
    # tmp_dp_1 = tmp_dp[mask]
    mask = tmp_dp.apply(lambda x: x.quantity is True, axis=1)
    return tmp_dp[mask]

def get_truncated_normal(mean=0, sd=1, low=0, upp=10):
    return truncnorm(
        (low - mean) / sd, (upp - mean) / sd, loc=mean, scale=sd)


def binarize(input_arr: np.ndarray,  threshold: float):
    output_arr = np.array([(1 if e > threshold else 0) for e in input_arr])
    return output_arr


def analyze(num, freq, d_type=0):
    """
        TODO:
            decide d_type and delta
            not just normal distribution.

        sigma = delta*x
        x may be changed
    """
    avg, delta = freq, 0.5
    mean, sigma = float(avg), delta*0.8

    x = get_truncated_normal(mean=mean, sd=sigma, low=0, upp=1)

    x = x.rvs(num)
    x_binarized = binarize(input_arr=x, threshold=0.5)

    # for visualize
    # plot([x.rvs(100000)])

    return bool(x_binarized)


# core function
def quantify(data):
    """
        TODO:
            1. generate data from statistic (analyze)
            2. filter by quantity (True)
    """
    print(data)

    q_data = data.apply(lambda x: pd.Series({'quantity': analyze(
        num=1,
        freq=x.item_frequency,
        d_type=0,
    )}), axis=1)

    return mask_by_quantity(data, q_data)

