"""
    TODO: may be structure Visualizer class in order to visualize only one time.
"""

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from util.build_dataset import build_master


def plot(data: list):
    print("in plot, the number of data set ", len(data))
    data_len = len(data)
    if data_len is 1:
        plt.hist(data[0], density=True)

    else:
        fig, ax = plt.subplots(data_len, sharex='True')
        for idx, arr in enumerate(data):
            ax[idx].hist(arr, density=True)

    plt.show()


def show_hist(series_list, name: str):
    # for jupyter notebook
    plt.title('{name} histogram'.format(name=name))
    return list(map(lambda ser: sns.distplot(ser.rename(name)), series_list))


def main():
    filter_origin_df = build_master(dataset="filter_origin_fmp", date="201908")
    clean_origin_df = build_master(dataset="clean_origin_fmp", date="201908")
    pass


if __name__ == '__main__':
    main()
