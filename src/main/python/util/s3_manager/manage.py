import tempfile
from io import StringIO, BytesIO

import boto3
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from joblib import dump, load

from util.logging import init_logger


class S3Manager:
    def __init__(self, bucket_name):
        """
        TODO:
            Add capability to process other formats (i.e. json, text, avro, parquet, etc.)
        :param bucket_name: AWS S3 bucket name
        """
        self.logger = init_logger()

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
        :param key: prefix
        :return: list of pd DataFrames
        """
        # filter
        objs_list = self.fetch_objs_list(prefix=key)
        filtered = list(filter(lambda x: x.size > 0, objs_list))

        def to_df(x):
            """
                transform obj(x) to df
            :param x: s3.ObjectSummery
            :return: bool
            """
            ls = StringIO(x.get()['Body'].read().decode('euc-kr'))
            tmp_df = pd.read_csv(ls, header=0)
            return tmp_df

        f_num = len(filtered)
        if f_num > 0:
            # test partial filtered by index slicing
            df_list = list(map(to_df, filtered))

            self.logger.info("{num} files is loaded from {dir} in s3 '{bucket_name}'".format(
                num=f_num, dir=key, bucket_name=self.bucket_name))
            return df_list
        else:
            # TODO: error handling
            raise Exception("nothing to be loaded in '{dir}'".format(dir=key))

    def save_object(self, body, key):
        """
            save to s3
        """
        self.s3.Object(bucket_name=self.bucket_name, key=key).put(Body=body)

        if len(self.fetch_objs_list(prefix=key)) is not 1:
            # if there is no saved file in s3, raise exception
            raise IOError("SaveError")
        else:
            self.logger.info("success to save '{key}' in s3 '{bucket_name}'".format(
                key=key, bucket_name=self.bucket_name
            ))

    def save_df_to_csv(self, df: pd.DataFrame, key: str):
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        self.save_object(body=csv_buffer.getvalue().encode('euc-kr'), key=key)

    def save_dump(self, x, key: str):
        with tempfile.TemporaryFile() as fp:
            dump(x, fp)
            fp.seek(0)
            self.save_object(body=fp.read(), key=key)
            fp.close()

    def load_dump(self, key: str):
        with tempfile.TemporaryFile() as fp:
            self.s3_bucket.download_fileobj(Fileobj=fp, Key=key)
            fp.seek(0)
            x = load(fp)
            fp.close()
        return x

    def save_plt_to_png(self, key):
        img_data = BytesIO()
        plt.savefig(img_data, format='png')
        img_data.seek(0)

        self.save_object(body=img_data, key=key)
        plt.figure()

