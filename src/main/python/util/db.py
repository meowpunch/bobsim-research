import logging
import sys
import pymysql.err
import pandas as pd
import pymysql
import yaml

from util.executable import get_destination


def get_connection():
    credentials_path = 'config/credentials.yaml'
    with open(get_destination(credentials_path)) as file:
        credentials = yaml.load(file, Loader=yaml.FullLoader)

    db_config = {
        'host': credentials['rds']['url'],
        'user': credentials['rds']['username'],
        'passwd': credentials['rds']['password'],
        'db': credentials['rds']['name'],
        'connect_timeout': 5,
        'charset': 'utf8'
    }

    logging.basicConfig()
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    try:
        return pymysql.connect(**db_config)

    except pymysql.MySQLError as e:
        logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
        logger.error(e)
        sys.exit()


def load_return_query(filename):
    destination_path = 'sql/' + filename
    with open(get_destination(destination_path)) as file:
        query = file.read()
        return pd.read_sql_query(query, get_connection())


def load_void_query(filename):
    destination_path = 'sql/' + filename

    with open(get_destination(destination_path)) as file:
        query = file.read()
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(query)
            conn.commit()

        finally:
            conn.close()




