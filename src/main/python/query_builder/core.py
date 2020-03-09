from util.db import load_return_query, load_void_query
from util.s3 import list_bucket_contents


class QueryBuilder:

    # Define function to use in common

    def __init__(self, table_name):
        self.check_sql_filename = 'desc_{}'.format(table_name)
        self.table_name = '{}'.format(table_name)

    def execute(self):
        return self.process()

    # TODO: let's think , do you really need it ?
    def process(self):
        raw_data = self.load_return_data(sql_name=str)
        print(raw_data)

    def load_return_data(self, sql_name):
        return load_return_query(sql_name)

    def load_void_data(self, name):
        try:
            load_void_query(name)

        except Exception as ex:
            print('error occur : ', ex)

    def check_data(self, sql_name):
        raw_data = self.load_return_data(sql_name)
        print(raw_data)


class CreateTable(QueryBuilder):
    # get Table name & switch to 'create_' form
    def __init__(self, table_name):
        self.create_table_name = 'create_{}.sql'.format(table_name)

        super().__init__(table_name)

    def execute(self):
        # void data executing
        QueryBuilder.load_void_data(self.create_table_name)
        # check void data frame
        return QueryBuilder.check_data(self.check_sql_filename)

 # TODO: make new Insert table class
