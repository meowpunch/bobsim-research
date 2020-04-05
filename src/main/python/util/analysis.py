from scipy.stats import skew


def get_skews(df):
    filtered = df.dtypes[(df.dtypes != "datetime64[ns]") & (df.dtypes != "object")].index

    return df[filtered].apply(lambda x: skew(x))

