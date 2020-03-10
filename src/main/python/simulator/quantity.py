"""
    first, just consider not the number but the existence of items.

    1. load item(food)
    2. make distribution of each item and randomly choose values
    3. binarize values by threshold
    4.
"""
import numpy as np
from scipy.stats import truncnorm

from util.visualize import plot


def get_truncated_normal(mean=0, sd=1, low=0, upp=10):
    return truncnorm(
        (low - mean) / sd, (upp - mean) / sd, loc=mean, scale=sd)


def binarize(input_arr: np.ndarray,  threshold: float):
    output_arr = np.array([(1 if e > threshold else 0) for e in input_arr])
    return output_arr


def quantify(num, freq, d_type=0):
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

    return x_binarized


