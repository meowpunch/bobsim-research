import logging
import sys

import pandas as pd
import pymysql
import pymysql.err
import yaml

from util.executable import get_destination


def get_connection(schema_name: str = "bobsim_schema"):
    credentials_path = 'config/credentials.yaml'
    with open(get_destination(credentials_path)) as file:
        credentials = yaml.load(file, Loader=yaml.FullLoader)

    db_config = {
        'host': credentials['rds'][schema_name]['url'],
        'user': credentials['rds'][schema_name]['username'],
        'passwd': credentials['rds'][schema_name]['password'],
        'db': schema_name,
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


def load_query(filename, prefix=''):
    destination_path = 'sql/' + prefix + filename
    # For read KOR , add encoding='utf-8'
    with open(get_destination(destination_path), encoding='utf-8') as file:
        query = file.read()
        return query


def exec_return_query(query, schema_name):
    return pd.read_sql_query(query, get_connection(schema_name))


def exec_void_query(args, query, schema_name):
    conn = get_connection(schema_name)
    try:
        with conn.cursor() as cur:
            if type(args[0]) is tuple and len(args) > 1:
                cur.executemany(query=query, args=args)
            else:
                cur.execute(query=query, args=args)
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


def show_data(query, schema_name):
    column = []
    column1 = []

    conn = get_connection(schema_name)
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
