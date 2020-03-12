import logging
import sys
import pymysql.err
import pandas as pd
import pymysql
import yaml
import csv

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


def load_query(filename):
    destination_path = 'sql/' + filename
    # For read KOR , add encoding='utf-8'
    with open(get_destination(destination_path), encoding='utf-8') as file:
        query = file.read()
        return query


def exec_return_query(query):
    return pd.read_sql_query(query, get_connection())


def exec_void_query(query):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(query)
        conn.commit()

    finally:
        conn.close()


def show_columns(query):  # get list(column_name) without id

    column = []
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

        for i in range(1, len(rows)):
            column.append(rows[i][0])

        conn.commit()

    finally:
        conn.close()
        return column


def show_data(query):
    column = []
    column1 = []

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

            def append_to_column(li):
                column1.append(rows[rows.index(li)])
                return column1

        column2 = list(map(append_to_column, rows[1:]))

        conn.commit()

    finally:
        conn.close()
        return column2
