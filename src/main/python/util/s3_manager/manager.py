from functools import reduce
from io import StringIO

import boto3
import pandas as pd


class S3Manager:
    def __init__(self, bucket_name):
        """
        TODO:
            Add capability to process other formats (i.e. json, text, avro, parquet, etc.)
            Add a func that write file to AWS S3
        :param bucket_name: AWS S3 bucket name
        """
        self.bucket_name = bucket_name

        self.s3 = boto3.resource('s3')
        self.s3_bucket = self.s3.Bucket(bucket_name)

    def fetch_objs_list(self, prefix):
        """
        :param prefix: filter by the prefix
        :return: list of s3.ObjectSummery
        """
        return list(self.s3_bucket.objects.filter(Prefix=prefix))

    def fetch_objects(self, key):
        """
            # TODO: consideration about one df OR empty list return
        :return: list of pd DataFrames
        """
        # init return variable
        df_list = list()

        # filter
        objs_list = self.fetch_objs_list(prefix=key)
        filtered = list(filter(lambda x: x.size > 0, objs_list))

        def read(x):
            """
                read csv file

                TODO: error handling
            :param x: s3.ObjectSummery
            :return: bool
            """
            ls = StringIO(x.get()['Body'].read().decode('euc-kr'))
            tmp_df = pd.read_csv(ls, header=0)
            return tmp_df

        if len(filtered) > 0:
            # test partial filtered by index slicing
            df_list = list(map(read, filtered[0:12]))

        return df_list

    def save_object(self, to_save_df, key):
        """
            save one object
        :param to_save_df:
        :param key:
        :return: success
        """
        csv_buffer = StringIO()
        to_save_df.to_csv(csv_buffer)
        self.s3.Object(bucket_name=self.bucket_name, key=key).put(Body=csv_buffer.getvalue())

        if len(self.fetch_objs_list(prefix=key)) is not 1:
            raise Exception("fail to save")

