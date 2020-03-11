from util.alter import *
from util.db import exec_return_query, exec_void_query, load_query
from util.s3 import list_bucket_contents

from abc import *


class QueryBuilder(metaclass=ABCMeta):

    # def __init__(self, table_name):
    def __init__(self, table_name, att_name: str = None,
                 insert_value: str = None, update_value: str = None,
                 where_clause: str = None, group_by=None,
                 having=None, order_by: str = None,
                 limit: int = None, offset: int = None):
        self.init_dict = {"TABLE_NAME": None,
                          "ATT_NAME": None,
                          "INSERT_VALUE": None,
                          "WHERE": None,
                          "GROUP_BY": None,
                          "ORDER_BY": None,
                          "HAVING": None,
                          "LIMIT": None,
                          "UPDATE_VALUE": None,
                          "OFFSET": None}

        self.init_dict.update({"TABLE_NAME": table_name,
                               "ATT_NAME": att_name,
                               "WHERE": where_clause,
                               "GROUP_BY": group_by,
                               "HAVING": having,
                               "ORDER_BY": order_by,
                               "LIMIT": limit,
                               "OFFSET": offset,
                               "UPDATE_VALUE": update_value,
                               "INSERT_VALUE": insert_value})
        # self.table_name = table_name
        self.query = None

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

        # mani_query = self.query.format(self.input_data)  # 2
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

    # def __init__(self, table_name, assignment_list, where_condition, order_con):
    #     super().__init__(table_name)
    #     self.update_table_name = 'update_{}.sql'.format(table_name)
    #     self.assignment_list = assignment_list
    #     self.where_condition = where_condition

    def execute(self):
        self.process()

    def process(self):
        """
                1. Store sql
                2. check whether where_condition is null
                    ( input : ' ' shape ) so, do not have to check!
                3. Manipulate sql
                4. exec sql
                5. check data in db
        """

        # self.store_query(self.update_table_name)  # 1

        self.store_query('update.sql')  # 1

        completed_query = self.manipulate(self.query)

        self.exec_query(completed_query)

        return self.check_update()

    def manipulate(self, query):
        if bool(self.init_dict.get("WHERE")) == 1:
            rest_data = self.init_dict["WHERE"]
        else:
            rest_data = str()

        first_mani_query = query % (self.init_dict["TABLE_NAME"], self.init_dict["UPDATE_VALUE"])

        second_mani_query = combine_sentence(first_mani_query, rest_data)

        return second_mani_query

    def exec_query(self, query):
        exec_void_query(query)

    def check_update(self):
        if bool(self.init_dict.get("WHERE")) == 1:
            check_update_query = SelectBuilder(self.init_dict["TABLE_NAME"], att_name='*',
                                               insert_value=self.init_dict["INSERT_VALUE"],
                                               where_clause=self.init_dict["WHERE"] + " AND " + self.init_dict["UPDATE_VALUE"],
                                               )
        else:
            check_update_query = SelectBuilder(self.init_dict["TABLE_NAME"], att_name='*',
                                               insert_value=self.init_dict["INSERT_VALUE"],
                                               where_clause="WHERE", update_value=self.init_dict["UPDATE_VALUE"])
        print(check_update_query.execute())


class SelectBuilder(ReturnQueryBuilder):

    def execute(self):
        return self.process()

    def process(self):
        """
                 1. Store select sql
                 2. manipulate sql
                 3. exec sql
        """
        self.store_query('select.sql')  # 1
        # TODO : add difficulty att in recipe table
        query = self.manipulate(self.query)  # 2

        return self.exec_query(query)

    def manipulate(self, query):  # extract from where_clause to offset
        """
            1.dict -> list
            2.add att,table name to 'SELECT {} FROM {}' sql file
            3.remove None & list -> str
            4.combine
        """

        rest_data = alter_type_dict_to_list(self.init_dict, 2, len(self.init_dict))

        first_mani_query = query % (self.init_dict["ATT_NAME"], self.init_dict["TABLE_NAME"])

        clean_data = remove_none(rest_data)

        str_rest_data = alter_type_list_to_str(clean_data)

        second_mani_query = first_mani_query + ' ' + str_rest_data

        return second_mani_query

    def exec_query(self, query):
        return exec_return_query(query)
