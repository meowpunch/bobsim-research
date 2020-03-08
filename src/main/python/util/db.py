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


def load_query(filename):
    destination_path = 'sql/' + filename
    with open(get_destination(destination_path)) as file:
        # 변경 필요
        query = file.read()
        # for debugging
        print(query)
        # if pd.read_sql_query get void query , return NonType(cause error)
        # so, we have to check query is void query or not

        check_create = "CREATE TABLE"
        if query.find(check_create) == -1:
            return pd.read_sql_query(query, get_connection())

        else:
            try:
                print('here2')
                ## ERROR HERE!!!
                cur = get_connection().cursor()
                    # debugging
                if cur.execute(query) == 0 :
                    print("Table already exists")

                else:
                    return cur.execute(query)

            except Exception as err:
                      print(err)
                      print("Error Code:", err.errno)
                      print("SQLSTATE", err.sqlstate)
                      print("Message", err.msg)

           # else:
           #           print("Success create table!!")

            finally:
                # resources closing
                    get_connection().commit()
                    get_connection().close()
