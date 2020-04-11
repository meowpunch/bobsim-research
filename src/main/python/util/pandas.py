import pandas as pd


def get_numeric_df(df: pd.DataFrame):
    return df.select_dtypes(exclude=['object', 'datetime64[ns]'])
