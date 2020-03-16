import functools

from util.executable import get_destination, load_file_list
import os

from util.s3 import save_to_s3


def load_csv(filename):
    return get_destination('csv/' + filename)


def save_csv(filename):
    """
        TODO: check already saved files and not overwrite

        1. delete korean in filename
        2. get year and make key(dir)
    :param filename: e.g. '원천조사가격정보_201501.csv'
    :return: e.g. '201501.csv
    """
    new = filename[9:]
    year = new[:4]
    key = 'price-predictor/origin/csv/' + year + '/' + new
    save_to_s3(key=key, body=load_csv(filename))
    return new


def make_origin_bucket():
    """
    :return: list of changed file names.
    """
    file_list = load_file_list(directory='csv')
    new_list = list(map(save_csv, file_list))
    return print("success save all ", new_list)







