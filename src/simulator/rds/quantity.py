"""
    first, just consider not the number but the existence of items.


    1. load item(food)
    2. make distribution of each item
"""
import sys
import logging
from rds import rds_config
# from rds_config import *
# from src/constant import rds_config
import pymysql
import numpy as np
import matplotlib.pyplot as plt
from scipy.stats import truncnorm


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


def get_truncated_normal(mean=0, sd=1, low=0, upp=10):
    return truncnorm(
        (low - mean) / sd, (upp - mean) / sd, loc=mean, scale=sd)


def binarize(input_arr: np.ndarray,  threshold: float):
    output_arr = np.array([(1 if e > threshold else 0) for e in input_arr])
    return output_arr


def func(tup):
    name, avg = tup
    print(name, avg)

    mean, sigma = float(avg), 0.5
    print(mean, sigma)
    # a = sigma * np.random.randn(10000) + mean
    a = np.random.normal(mean, sigma, 10)
    print(a)
    # count, bins, ignored = plt.hist(a, 30, density=True)
    #plt.plot(bins, 1 / (sigma * np.sqrt(2 * np.pi)) *
    #         np.exp(- (bins - mean) ** 2 / (2 * sigma ** 2)), linewidth=2, color='r')

    # plt.hist(a, bins=100, density=True, alpha=1, histtype='step', label='(mean, stddev)=('+str(mean)+', '+str(sigma*sigma)+')')

    X = get_truncated_normal(mean=mean, sd=sigma**2, low=0, upp=1)

    x = X.rvs(100)
    print(type(x))

    print(x)
    fig, ax = plt.subplots(2, sharex=True)
    ax[0].hist(x, density=True)

    x = binarize(input_arr=x, threshold=0.5)

    print(x)
    ax[1].hist(x, density=True)
    plt.show()




def handler():

    conn = db_load()

    """
        This function fetches content from MySQL RDS instance
    """

    with conn.cursor() as cur:
        """
            cur.execute("create table Employee ( EmpID  int NOT NULL, Name varchar(255) NOT NULL, PRIMARY KEY (EmpID))")
            cur.execute('insert into Employee (EmpID, Name) values(1, "Joe")')
            cur.execute('insert into Employee (EmpID, Name) values(2, "Bob")')
            cur.execute('insert into Employee (EmpID, Name) values(3, "Mary")')
            conn.commit()
            cur.execute("select * from Employee")
            for row in cur:
                item_count += 1
                logger.info(row)
                # print(row)
        """

        query = "select item.name, item_frequency from item right outer join item_frequency on item.id = item_frequency.item_id"

        result = cur.execute(query)
        print(cur)

        count = 0
        for row in cur:
            if count is 0:
                func(row)
            count += 1



        print("뭐지")
        print(result)

        # cur.execute("drop table Employee")
    conn.commit()

    # print("Added %d items from RDS MySQL table" % item_count)

