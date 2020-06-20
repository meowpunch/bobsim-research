import json
import tempfile
from io import StringIO, BytesIO

import boto3
import matplotlib.pyplot as plt
import pandas as pd
from joblib import dump, load

from utils.logging import init_logger


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

    def fetch_objects(self, key, conversion_type):
        """
            # TODO: consideration about one df OR empty list return
        :param key: directory in s3_bucket
        :param conversion_type:
                        "df_from_csv", "dict_from_json"
        :return:
        """
        # filter
        objs_list = self.fetch_objs_list(prefix=key)
        filtered = list(filter(lambda o: o.size > 0 and conversion_type in o.key, objs_list))

        def convert(c_type):
            return{
                "csv": lambda obj: pd.read_csv(StringIO(obj.get()['Body'].read().decode('euc-kr')), header=0),
                "json": lambda obj: json.loads(obj.get()['Body'].read().decode('utf-8'))
            }[c_type]

        f_num = len(filtered)
        if f_num > 0:
            # test partial filtered by index slicing
            data_list = list(map(convert(c_type=conversion_type), filtered))

            self.logger.info("{num} files is loaded from {dir} in s3 '{bucket_name}'".format(
                num=f_num, dir=key, bucket_name=self.bucket_name))
            return data_list
        else:
            # TODO: error handling
            self.logger.debug("nothing to be loaded in '{dir}'".format(dir=key))
            return None

    def fetch_dict_from_json(self, key):
        return self.fetch_objects(key=key, conversion_type="json")

    def fetch_df_from_csv(self, key):
        return self.fetch_objects(key=key, conversion_type="csv")

    def save_object(self, body, key, kwargs=None):
        """
        :param body: data
        :param key: directory in s3
        :param kwargs: other arguments ex. 'ACL', 'ContentType'
        :return: success code
        """
        if kwargs is None:
            self.s3.Object(bucket_name=self.bucket_name, key=key).put(Body=body)
        else:
            self.s3.Object(bucket_name=self.bucket_name, key=key).put(**kwargs, Body=body)

        if len(self.fetch_objs_list(prefix=key)) is not 1:
            # if there is no saved file in s3, raise exception
            return False
        else:
            self.logger.info("success to save '{key}' in s3 '{bucket_name}'".format(
                key=key, bucket_name=self.bucket_name
            ))
            return True

    def save_dict_to_json(self, data: dict, key: str):
        serialized_data = json.dumps(data, ensure_ascii=False)
        return self.save_object(key=key, body=serialized_data)

    def save_df_to_csv(self, df: pd.DataFrame, key: str):
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        return self.save_object(body=csv_buffer.getvalue().encode('euc-kr'), key=key)

    def save_img(self, data, key, kwargs):
        return self.save_object(body=data, key=key, kwargs=kwargs)

    def save_dump(self, x, key: str):
        with tempfile.TemporaryFile() as fp:
            dump(x, fp)
            fp.seek(0)
            code = self.save_object(body=fp.read(), key=key)
            fp.close()
        return code

    def load_dump(self, key: str):
        with tempfile.TemporaryFile() as fp:
            self.s3_bucket.download_fileobj(Fileobj=fp, Key=key)
            fp.seek(0)
            x = load(fp)
            fp.close()
        self.logger.info("success to download from '{key}'".format(key=key))
        return x

    def save_plt_to_png(self, key):
        img_data = BytesIO()
        plt.savefig(img_data, format='png')
        img_data.seek(0)

        code = self.save_object(body=img_data, key=key)
        plt.figure()
        return code

