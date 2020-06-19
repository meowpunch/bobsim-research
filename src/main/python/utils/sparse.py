import pandas as pd


def filter_sparse(column: pd.Series, std_list: list):
    sparse_list = list(filter(lambda x: x not in std_list, column.unique()))
    return column.replace(sparse_list, "others")
