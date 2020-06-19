"""
    TODO: may be structure Visualizer class in order to visualize only one time.
"""
import sys

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from utils.logging import init_logger


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


def series_plot(ser: pd.Series, kind="bar", y_label="y", x_label="x", title="Untitled", d=0.1):
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    ser_max, ser_min = ser.max(), ser.min()
    init_logger().info("min/max: {min}/{max}".format(min=ser_min, max=ser_max))
    delta = (ser_max - ser_min) * d
    ser.plot(kind=kind, ylim=(ser_min - delta, ser_max + delta), title=title)
    plt.show()


def draw_hist(s, h_type: str = "dist", name: str = None):
    h_method = {
        "dist": sns.distplot,
        "count": sns.countplot,
    }
    try:
        method = h_method[h_type]
    except KeyError:
        # TODO: handle exception
        init_logger().critical("histogram type '{h_type}' is not supported".format(h_type=h_type))
        sys.exit()

    if isinstance(s, pd.Series):
        plt.title('{name} histogram'.format(name=s.name))
        method(s)
        plt.show()
    else:
        # for jupyter notebook
        plt.title('{name} histogram'.format(name=name))
        return list(map(lambda series: method(series), s))


def main():
    pass


if __name__ == '__main__':
    main()
