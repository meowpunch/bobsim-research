from functools import reduce
from io import StringIO

import boto3
import pandas as pd

from data_pipeline.dtype import public_price


class S3Manager:
    def __init__(self, bucket_name, key):
        """
        TODO:
            Add capability to process other formats (i.e. json, text, avro, parquet, etc.)
            Add a func that write file to AWS S3
        :param bucket_name: AWS S3 bucket name
        :param key: AWS S3 key to locate the CSV file
        """
        self.bucket_name = bucket_name
        self.key = key

        self.s3 = boto3.resource('s3')
        self.s3_bucket = self.s3.Bucket(bucket_name)

    def fetch_objs_list(self, prefix):
        return list(self.s3_bucket.objects.filter(Prefix=self.key))


    def fetch_objects(self):
        """

        """
        df = pd.DataFrame()
        temp = list(self.s3_bucket.objects.filter(Prefix=self.key))
        filtered = list(filter(lambda x: x.size > 0, temp))

        def read(x):
            """
                TODO: error handling
                      1. read
                      2. check column
                      3. concat
            :param x: s3.ObjectSummery
            :return: bool
            """
            # 1
            ls = StringIO(x.get()['Body'].read().decode('euc-kr'))
            tmp_df = pd.read_csv(ls, header=0, dtype=public_price)
            return tmp_df

        if len(filtered) > 0:
            df_list = list(map(read, filtered))

        # def merge(x, y):
        #     if x.columns is y.columns:
        #
        #     return

        def is_valid_columns(x):

            return
        list(filter(lambda x: x.columns is not df_list[0].columns, df_list))


        def merge(x, y)
            return x==y
        reduce(merge, df_list)


        # first of df_list is standard columns
        #


        #  print(df_list[0].dtypes)
        return df

    def execute(self):
        """
            Now, this manager can only read CSV file from AWS S3
            read & turn it into pandas data frame
        :return: pd DataFrame
        """
        return self.fetch_objects()

