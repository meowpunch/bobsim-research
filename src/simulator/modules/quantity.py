"""
    first, just consider not the number but the existence of items.

    1. load item(food)
    2. make distribution of each item and randomly choose values
    3. binarize values by threshold
    4.
"""
import numpy as np
from scipy.stats import truncnorm
import math

from modules import visualize


def get_truncated_normal(mean=0, sd=1, low=0, upp=10):
    return truncnorm(
        (low - mean) / sd, (upp - mean) / sd, loc=mean, scale=sd)


def binarize(input_arr: np.ndarray,  threshold: float):
    output_arr = np.array([(1 if e > threshold else 0) for e in input_arr])
    return output_arr


def quantify(num, freq, d_type=0):
    avg, delta = freq, 0.5
    print(avg)

    mean, sigma = float(avg), delta*0.5
    print(mean, sigma)
    # a = sigma * np.random.randn(10000) + mean
    # a = np.random.normal(mean, sigma, 10)
    # print(a)

    x = get_truncated_normal(mean=mean, sd=sigma, low=0, upp=1)
    x = x.rvs(num)
    x_binarized = binarize(input_arr=x, threshold=0.5)

    # print(type([x, x_binarized]))
    # visualize.plot(data=[x, x_binarized])
    return x_binarized


