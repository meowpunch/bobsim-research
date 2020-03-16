import boto3
import json

import yaml

from util.executable import get_destination


def get_url_s3():
    credentials_path = 'config/credentials.yaml'
    with open(get_destination(credentials_path)) as file:
        credentials = yaml.load(file, Loader=yaml.FullLoader)
    return credentials['s3']['url']


def list_bucket_contents():
    s3 = boto3.resource('s3')

    for bucket in s3.buckets.all():
        print(bucket.name)


def list_objects(prefix):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket('production-bobsim')
    return bucket.objects.filter(Prefix=prefix)


def save_to_s3(key, body):
    s3 = boto3.resource('s3')
    return s3.Object(bucket_name='production-bobsim', key=key).put(Body=body)
    # s3.Object(bucket_name='production-bobsim', key=key).get()['Body'].read()


def save_json(directory, filename, data):
    """
    :param directory:
    :param filename:
    :param data:
    :return: load json saved in S3 (for check)
    """
    serialized_data = json.dumps(data, ensure_ascii=False)
    return save_to_s3(key=directory + "/" + filename, body=serialized_data)
