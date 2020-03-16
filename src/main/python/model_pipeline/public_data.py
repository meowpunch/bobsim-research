from util.general import load_csv, load_file_list
from util.s3 import save_to_s3


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


def origin_bucket_csv():
    """
    :return: list of changed file names.
    """
    file_list = load_file_list(directory='csv')
    new_list = list(map(save_csv, file_list))
    return print("success save all ", new_list)


if __name__ == '__main__':
    origin_bucket_csv()






