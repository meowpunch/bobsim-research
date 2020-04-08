"""
    TODO: may be structure Visualizer class in order to visualize only one time.
"""

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


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

def draw_hist(s, name: str = None):
    if isinstance(s, pd.Series):
        plt.title('{name} histogram'.format(name=s.name))
        sns.distplot(s)
    else:
        # for jupyter notebook
        plt.title('{name} histogram'.format(name=name))
        return list(map(lambda series: sns.distplot(series), s))


def main():
    pass


if __name__ == '__main__':
    main()
