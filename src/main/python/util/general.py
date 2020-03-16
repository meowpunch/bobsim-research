import os

from util.executable import get_destination


def load_csv(filename):
    return open(get_destination('csv/' + filename), 'rb')


def load_file_list(directory):
    destination_path = directory + '/'
    path = get_destination(destination_path)
    return os.listdir(path)