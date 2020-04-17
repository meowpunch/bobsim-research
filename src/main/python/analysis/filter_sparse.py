import pandas as pd


def get_sparse_list(df: pd.DataFrame, column: str, number: int):
    percent_df = (df[column].value_counts()/df[column].value_counts().sum()*100).rename("%").to_frame().sort_values(by="%")
    high_list = percent_df.sort_values(by='%', ascending=False,).head(number).index
    return high_list


def filter_sparse(df: pd.DataFrame, column: str, sparse_list: list, to_word: str = "others"):
    filter_list = list(set(list(df[column].unique())) - set(sparse_list))
    return df.replace(filter_list, to_word)


