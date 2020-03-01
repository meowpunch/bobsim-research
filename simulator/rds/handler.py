import sys
import logging
from .config import *
import pymysql


def handler():
    # rds settings
    rds_host = "production-bobsim-aurora.cluster-cm9kakdihdlv.ap-northeast-2.rds.amazonaws.com"
    name = db_username
    password = db_password
    database_name = db_name

    logging.basicConfig()
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    try:
        conn = pymysql.connect(rds_host, user=name, passwd=password, db=database_name, connect_timeout=5, charset='utf8')
    except pymysql.MySQLError as e:
        logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
        logger.error(e)
        sys.exit()

    logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")

    """
    This function fetches content from MySQL RDS instance
    """

    item_count = 0

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
        cur.execute("drop table Employee")
    conn.commit()

    # print("Added %d items from RDS MySQL table" % item_count)
