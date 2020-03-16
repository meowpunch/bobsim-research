

from util.db import load_query,exec_void_query,exec_return_query
from util.s3 import list_bucket_contents




class CreateTable:

    """
            TODO: logic comes here
            total:  0. get name for create table
                    1. sql store into sql filename
                    2. execute query ( sql)
                    3. throw (select or desc) query (created table)
                    4. return 1 : if raw_data exist
                       return 0 : if fqw_data non-exist

    """
    # class variable for test
    a = 'drop_temp.sql'

    # TODO : Creating instance with input(create_name)
    def __init__(self, create_name):
        self.create_sql_filename = 'create_{}.sql'.format(create_name)
        self.sql_filename = 'desc_{}.sql'.format(create_name)

    # TODO : excuting process(create & check table)
    def execute(self):
        return self.process()

    # TODO : Check create table
    def check_data(self):
        raw_data = self.load_data()
        print(raw_data)

    def load_data(self):
        return load_query(self.sql_filename)

    # TODO : creating table and check
    def process(self):
        try:
            load_query(self.create_sql_filename)

        except Exception as ex:
            print('error occur : ', ex)

    # TODO : VOID QUERY HANDLING ( because : result of create query is NonType )
        return self.check_data()





    # def execute_create(self):
    #     load_query()
    #
    #     # drop created table for debugging
    #     load_query(self.a)
    #     print(self.a)



    # def process(self):
    #    self.execute_create()
"""
    TODO : after executing , check desc create_sql_filename
"""


    # def load_data(self):
    #     return load_query(self.sql_filename)
    #
    # def process(self):
    #     raw_data = self.load_data()


