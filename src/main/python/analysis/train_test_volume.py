import pandas as pd


def set_train_test(df: pd.DataFrame, train_size=5, test_size=1):
    """
        [- train_size -)[- test_size -)[- remains -
    :param test_size:
    :param train_size:
    :param df: dataset
    :return: train Xy, test Xy
    """
    test_size = test_size
    train_size = train_size
    # TODO: it should be processed in data_pipeline
    time_series = df["date"].drop_duplicates().tolist()

    # split train & test
    train = df[df["date"].dt.date < time_series[train_size]]
    condition = \
        (time_series[train_size] <= df["date"].dt.date) & \
        (df["date"].dt.date < time_series[train_size + test_size])
    test = df[condition]
    return train, test
