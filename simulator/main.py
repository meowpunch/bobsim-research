import boto3
from rds.handler import handler

if __name__ == '__main__':

    # Let's use Amazon S3
    s3 = boto3.resource('s3')

    # Print out bucket names
    for bucket in s3.buckets.all():
        print(bucket.name)

    table_name = "Employee"
    handler();


