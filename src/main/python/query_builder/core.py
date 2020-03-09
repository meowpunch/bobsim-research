from util.db import exec_return_query, exec_void_query, load_query
from util.s3 import list_bucket_contents

from abc import *


class QueryBuilder(metaclass = ABCMeta):

    def __init__(self, table_name):
        self.table_name = table_name
        self.query = None
        # '{}'.format(table_name)

    def store_query(self, sql_filename):
        self.query = load_query(sql_filename)

    @abstractmethod
    def exec_query(self, query):
        pass

class VoidQueryBuilder(QueryBuilder):

    def exec_query(self, query):
        exec_void_query(query)


class ReturnQueryBuilder(QueryBuilder):

    def exec_query(self, query):
        exec_return_query(query)


# QueryBuilder-VoidQueryBuilder-CreateBuilder
class CreateBuilder(VoidQueryBuilder):

    def __init__(self, table_name):
        super().__init__(table_name)
        self.create_table_name = 'create_{}.sql'.format(self.table_name)

    def execute(self):
        self.process()

    def process(self):
        # TODO: 1. Store sql 2. Exec stored sql 3. check

        self.store_query(self.create_table_name)
        self.exec_query(self.query)
        return self.check_create()

    def check_create(self):
        check_name = 'desc_{}.sql'.format(self.table_name)
        check_query = load_query(check_name)
        return exec_return_query(check_query)


# QueryBuilder-VoidQueryBuilder-InsertBuilder
class InsertBuilder(VoidQueryBuilder):

    def __init__(self, table_name, input_data):
        super().__init__(table_name)
        self.insert_table_name = 'insert_{}.sql'.format(table_name)
        self.input_data = input_data

    def execute(self):
        self.process()

    def process(self):
        # TODO: 1. Store sql 2. manipulate sql 3. exec sql 4. check
        self.store_query(self.insert_table_name)  # 1

        mani_query = self.query.format(self.input_data)  # 2

        self.exec_query(mani_query)

        return self.check_insert()

    def check_insert(self):
        check_query_novalue = load_query('select_{}.sql'.format(self.table_name))
        # TODO : unpacking tuple & packing tuple // ((1,2,3)) -> (1,2,3)

        mani_data= check_query_novalue % self.input_data
        return exec_return_query(mani_data)




"""

    def load_create_query(self):
        return


    def load_return_data(self, sql_name):
        query = load_query(sql_name)

        return exec_return_query(query)

    def load_void_data(self, name):
        try:
            query = load_query(name)
            exec_void_query(name)

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
class InsertTable(QueryBuilder):
    # get Table name & switch to 'insert_' form
    def __init__(self, table_name, input_data ):

        self.insert_table_name = 'insert_{}.sql'.format(table_name)
        # input_data_shape : tuple type , tuple of tuple type
        self.input_data = input_data # tuple
        super.__init__(table_name)

"""






