import boto3
import json


def list_bucket_contents():
    s3 = boto3.resource('s3')

    for bucket in s3.buckets.all():
        print(bucket.name)


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


