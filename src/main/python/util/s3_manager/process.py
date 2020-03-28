from util.s3_manager.manager import S3Manager


def main():
    """
        standalone process to load and save CSV file data with AWS S3
    """
    manager = S3Manager(
        bucket_name="",
    )
    df_list = manager.fetch_objects(key="")

    print(df)


if __name__ == '__main__':
    main()