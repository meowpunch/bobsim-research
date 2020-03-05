
import numpy as np
import matplotlib.pyplot as plt


def plot(data: list):
    print(data)
    print(len(data))
    data_len = len(data)
    if data_len is 1:
        plt.hist(data[0], density=True)

    else:
        fig, ax = plt.subplots(data_len, sharex=True)
        for idx, arr in enumerate(data):
            ax[idx].hist(arr, density=True)

    plt.show()
