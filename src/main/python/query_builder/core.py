from util.db import exec_return_query, exec_void_query, load_query
from util.s3 import list_bucket_contents

from abc import *


class QueryBuilder(metaclass=ABCMeta):

    def __init__(self, table_name):
        self.table_name = table_name
        self.query = None
        self.init_dict = {"TABLE_NAME": None,
                          "ATT_NAME": None,
                          "INPUT_VALUE": None,
                          "WHERE": None,
                          "GROUP_BY": None,
                          "ORDER_BY": None,
                          "HAVING": None,
                          "LIMIT": None,
                          "UPDATE_VALUE": None,
                          "OFFSET": None}

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
        #  1. Store sql 2. Exec stored sql 3. check

        self.store_query(self.create_table_name)
        self.exec_query(self.query)
        return self.check_create()

    def check_create(self):
        check_name = 'desc_{}.sql'.format(self.table_name)
        check_query = load_query(check_name)
        return exec_return_query(check_query)


# QueryBuilder-VoidQueryBuilder-InsertBuilder
class InsertBuilder(VoidQueryBuilder):
    # InsertBuilder('table_name', 'input_data')
    def __init__(self, table_name, input_data):
        super().__init__(table_name)
        self.insert_table_name = 'insert_{}.sql'.format(table_name)
        self.input_data = input_data

    def execute(self):
        self.process()

    def process(self):
        #  1. Store sql 2. manipulate sql 3. exec sql 4. check
        self.store_query(self.insert_table_name)  # 1

        self.str_data = ', '.join(map(str, self.input_data))

        #mani_query = self.query.format(self.input_data)  # 2
        mani_query = self.query.format(self.str_data)  # 2
        self.exec_query(mani_query)  # 3

    #     return self.check_insert()  # 4
    #
    # def check_insert(self):
    #     # Load sql  SELECT {att_name} FROM {table_name} WHERE { ~ }
    #     check_query = load_query('check_insert_{}.sql'.format(self.table_name))
    #
    #     # TODO: ADD input_data to WHERE clause ( need to modify )
    #     self.str_data = ', '.join(map(str, self.input_data))
    #
    #     # mani_query = self.query.format(self.input_data)  # 2
    #     mani_query = check_query % self.str_data  # 2
    #     return exec_return_query(mani_query)

# QueryBuilder-VoidQueryBuilder-InsertBuilder
class DeleteBuilder(VoidQueryBuilder):
    # DeleteBuilder('table_name', 'input_data'or None)
    def __init__(self, table_name, input_data):
        super().__init__(table_name)
        self.delete_table_name = 'delete_{}.sql'.format(table_name)
        self.input_data = input_data

    def execute(self):
        self.process()

    def process(self):
        # TODO: 1. Store sql 2. manipulate sql 3. exec sql 4. check
        self.store_query(self.delete_table_name)  # 1

        mani_query = self.query.format(self.input_data)  # 2

        self.exec_query(mani_query)  # 3

        return self.check_delete()  # 4

    def check_delete(self):
        check_query = load_query('check_delete_{}.sql'.format(self.table_name))

        # Load sql  SELECT {} FROM {self.table_name} {} {} {}

        mani_query = check_query % ('*', '')
        print(mani_query)
        return exec_return_query(mani_query)

    # TODO : After make a SeleckBuilder , modify all of chekck func

# QueryBuilder-VoidQueryBuilder-UpdatetBuilder
class UpdateBuilder(VoidQueryBuilder):
    #  UpdateBuilder('table_name', 'assignment_list', 'where_condition' '
    def __init__(self, table_name, assignment_list, where_condition, order_con):
        super().__init__(table_name)
        self.update_table_name= 'update_{}.sql'.format(table_name)
        self.assignment_list = assignment_list
        self.where_condition = where_condition

    def execute(self):
        self.process()

    def process(self):
        """
        TODO :  1. Store sql
                2. check whether where_condition is null
                    ( input : ' ' shape ) so, do not have to check!
                3. Manipulate sql
                4. exec sql
                5. check data in db
        """

        self.store_query(self.update_table_name)  # 1

        mani_query = self.query.format(self.assignment_list, self.where_condition)  # 3

        self.exec_query(mani_query)

        return self.check_update()

    def check_update(self):
        # Whether data in table
        pass


class SelectBuilder(ReturnQueryBuilder):

    def __init__(self, att_name, table_name, where_clause=None,
                     group_by=None, having=None, order_by=None,
                     limit=None, offset=None):
        super().__init__(table_name)

        self.init_dict.update({"TABLE_NAME": table_name,
                               "ATT_NAME": att_name,
                               "WHERE": where_clause,
                               "GROUP_BY": group_by,
                               "HAVING": having,
                               "ORDER_BY": order_by,
                               "LIMIT": limit,
                               "OFFSET": offset})

    def execute(self):
        return self.process()

    def process(self):
        """
        # TODO : 1. Store select sql
                 2. manipulate sql
                 3. exec sql
        :return: select query data
        """
        self.store_query('select.sql')  # 1
    # TODO : add difficulty att in recipe table

        # extract from where_clause to offset

        #iterator = list(self.init_dict.values())[2:]
        #rest_data = list(map(lambda x: x, iterator))

        rest_data= alter_type_dict_to_list(self.init_dict, 2, len(self.init_dict))

        first_mani_query= self.query % (self.init_dict["ATT_NAME"], self.init_dict["TABLE_NAME"])
        clean_data = remove_none(rest_data)  # remove None & dict->list
        print(first_mani_query)
        print(clean_data)
        str_rest_data= '  '.join(map(str, clean_data))  # list -> str

        print(str_rest_data)

        second_mani_query = first_mani_query + ' ' + str_rest_data

        print(second_mani_query)
        return exec_return_query(second_mani_query)

    """
        TODO: 1. make func (dict->list, Romove None, list-> str )
              2. make func (list merge ) 
              3. modify classes ( create , insert , delete )
    
    """


def alter_type_dict_to_list(data=dict, start_interval=int, end_interval=int):
    # dict.value() -> list
    iterator = list(data.values())[start_interval:end_interval]
    list_data = list(map(lambda x: x, iterator))
    return list_data


# def remove_none(data=list):
#     clean_data = list(filter(partial(is_not, None), data))
#     # list(filter(None.__ne__, data))
#     return clean_data
