import os
import pandas as pd

from util.executable import get_destination


def load_file(filename):
    return open(get_destination(filename), 'rb')


def load_csv(filename):
    return open(get_destination('csv/' + filename), 'rb')


def load_csv_pandas(filename):
    return pd.read_csv(get_destination('csv/' + filename), encoding='euc-kr')


def load_file_list(directory):
    destination_path = directory + '/'
    path = get_destination(destination_path)
    return os.listdir(path)