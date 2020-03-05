import sys
import logging
import json

import pymysql
import numpy as np
import boto3

from modules.quantity import quantify
from modules.price import price
from constants import rds_config


def db_load():

    rds_host = rds_config.access_url
    name = rds_config.db_username
    password = rds_config.db_password
    database_name = rds_config.db_name

    logging.basicConfig()
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    try:
        conn = pymysql.connect(rds_host, user=name, passwd=password, db=database_name, connect_timeout=5,
                               charset='utf8')
    except pymysql.MySQLError as e:
        logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
        logger.error(e)
        sys.exit()

    logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")

    return conn


if __name__ == '__main__':

    # Let's use Amazon S3
    s3 = boto3.resource('s3')

    # Print out bucket names
    for bucket in s3.buckets.all():
        print(bucket.name)

    conn = db_load()

    """
        This function fetches content from MySQL RDS instance
    """

    with conn.cursor() as cur:

        query = "select item.name, item_frequency, average, delta, distr_type from item inner join item_frequency on item.id = item_frequency.item_id inner join price on item.id = price.item_id"
        result = cur.execute(query)
        print(cur)

        x_num = 10000
        count = 0

        # x = np.empty((2, 2))
        x_name = []

        fridge = dict()
        for row in cur:
            item_name, item_freq, price_avg, price_delta, price_d_type = row
            # print(row)
            if count < 3:
                print(row)
                x_quantity = quantify(num=x_num, freq=item_freq, d_type=0)
                print(x_quantity)
                x_price = price(num=x_num, avg=price_avg, delta=price_delta, d_type=price_d_type)

                x_tmp = np.column_stack([x_quantity, x_price])
                x_name.append(item_name)
                print(x_tmp.tolist())
                print(x_name)
                x = np.column_stack([x, x_tmp])
                print(x)
            count += 1

        print(x.shape)

    conn.commit()


    # print(__name__)


