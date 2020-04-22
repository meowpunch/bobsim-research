import pandas as pd

from util.build_dataset import build_origin_price
from util.s3_manager.manage import S3Manager


def get_std_list(column: pd.Series, number: int):
    return column.value_counts().head(number).index


def main():
    """
        save list of non-sparse item names (std_list).
    :return: exit code
    """
    # get standard list
    bucket_name = "production-bobsim"
    df, key = build_origin_price(bucket_name=bucket_name, date="201908")
    std_list = get_std_list(column=df["standard_item_name"], number=48)
    # print(std_list)

    # save standard list
    s3_manager = S3Manager(bucket_name=bucket_name)
    s3_manager.save_dump(
        x=std_list, key="food_material_price_predict_model/constants/std_list.pkl"
    )
    return 0


if __name__ == '__main__':
    main()
