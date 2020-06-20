from utils.s3_manager.manage import S3Manager


def main():
    """
        standalone process to load and save CSV file data with AWS S3
    """
    manager = S3Manager(
        bucket_name="production-bobsim",
    )
    objs = manager.fetch_objects(key="crawled_recipe", conversion_type="json")
    print(objs)


if __name__ == '__main__':
    main()
