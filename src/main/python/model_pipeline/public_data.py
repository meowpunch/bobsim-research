from util.executable import get_destination
import os


def load_csv():
    pass


def change_filename(folder):

    destination_path = folder + '/'

    fl = os.listdir(destination_path)
    print(type(fl))

