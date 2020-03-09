import boto3
import json


def list_bucket_contents():
    s3 = boto3.resource('s3')

    for bucket in s3.buckets.all():
        print(bucket.name)


def save_json(directory, filename, data):
    s3 = boto3.resource('s3')
    serialized_data = json.dumps(data)

    key = directory + "/" + filename
    s3.Object(bucket_name='production-bobsim', key=key).put(Body=serialized_data)
