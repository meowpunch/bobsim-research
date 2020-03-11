from util.alter import *
from util.db import exec_return_query, exec_void_query, load_query
from util.s3 import list_bucket_contents

from abc import *


class QueryBuilder(metaclass=ABCMeta):

    # def __init__(self, table_name):
    def __init__(self, table_name, att_name: str = None,
                 value: str = None, where_clause: str = None, and_test=None,
                 group_by=None,
                 having=None, order_by: str = None,
                 limit: int = None, offset: int = None):
        self.init_dict = {"TABLE_NAME": None,
                          "ATT_NAME": None,
                          "VALUE": None,
                          "WHERE": None,
                          "AND": None,
                          "GROUP_BY": None,
                          "ORDER_BY": None,
                          "HAVING": None,
                          "LIMIT": None,
                          "OFFSET": None}

        self.init_dict.update({"TABLE_NAME": table_name,
                               "ATT_NAME": att_name,
                               "VALUE": value,
                               "WHERE": where_clause,
                               "AND": and_test,
                               "GROUP_BY": group_by,
                               "HAVING": having,
                               "ORDER_BY": order_by,
                               "LIMIT": limit,
                               "OFFSET": offset,
                               })
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

    def execute(self):
        self.process()

    def process(self):
        #  1. Store sql 2. manipulate sql 3. exec sql 4. check
        self.store_query('insert.sql')  # 1

        query = self.manipulate(self.query)

        self.exec_query(query)

    #     return check_insert()
    #
    # def check_insert(self):
    #     self.init_dict["ATT"]
    #     list(filter())
    #     pass
    def manipulate(self, query):

        mani_query = query % self.init_dict["TABLE_NAME"], self.init_dict["ATT_NAME"],\
                             self.init_dict["VALUE"]

        return mani_query

    def exec_query(self, query):
        exec_void_query(query)

# QueryBuilder-VoidQueryBuilder-InsertBuilder
class DeleteBuilder(VoidQueryBuilder):

    def execute(self):
        self.process()

    def process(self):
        # TODO: 1. Store sql 2. manipulate sql 3. exec sql 4. check
        self.store_query('delete.sql')  # 1

        query = self.manipulate(self.query)  # 2

        self.exec_query(query)  # 3

        return self.check_delete()  # 4

    def manipulate(self, query):

        first_mani_query = query % self.init_dict["TABLE_NAME"]

        list_rest_data = alter_type_dict_to_list(self.init_dict, 3, len(self.init_dict))

        clean_rest_data = remove_none(list_rest_data)

        str_rest_data = alter_type_list_to_str(clean_rest_data)

        second_mani_query = combine_sentence(first_mani_query, str_rest_data)

        return second_mani_query

    def exec_query(self, query):
        exec_void_query(query)

    def check_delete(self):
        check_delete_query = SelectBuilder(self.init_dict["TABLE_NAME"], ' * ')

        print(check_delete_query.execute())


# QueryBuilder-VoidQueryBuilder-UpdatetBuilder
class UpdateBuilder(VoidQueryBuilder):

    def execute(self):
        self.process()

    def process(self):
        """
                1. Store sql
                2. Manipulate sql
                3. exec sql
                4. check data in db
        """

        self.store_query('update.sql')  # 1

        completed_query = self.manipulate(self.query)  # 2

        self.exec_query(completed_query)  # 3

        return self.check_update()  # 4

    def manipulate(self, query):
        """
            TODO: 1. update {table_name} set {update_value} % merge
                  2. clean

        """
        first_mani_query = query % (self.init_dict["TABLE_NAME"], self.init_dict["VALUE"])

        list_rest_data = alter_type_dict_to_list(self.init_dict, 3, len(self.init_dict))

        clean_rest_data = remove_none(list_rest_data)

        str_rest_data = alter_type_list_to_str(clean_rest_data)

        second_mani_query = combine_sentence(first_mani_query, str_rest_data)

        return second_mani_query

    def exec_query(self, query):
        exec_void_query(query)

    def check_update(self):
        if bool(self.init_dict.get("WHERE")) == 1:
            check_update_query = SelectBuilder(self.init_dict["TABLE_NAME"], '*',
                                               where_clause=self.init_dict["WHERE"]+" AND "+self.init_dict["VALUE"])
        else:
            check_update_query = SelectBuilder(self.init_dict["TABLE_NAME"], '*',
                                               where_clause="WHERE " + self.init_dict["VALUE"])
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

        rest_data = alter_type_dict_to_list(self.init_dict, 3, len(self.init_dict))

        first_mani_query = query % (self.init_dict["ATT_NAME"], self.init_dict["TABLE_NAME"])

        clean_data = remove_none(rest_data)

        str_rest_data = alter_type_list_to_str(clean_data)

        second_mani_query = combine_sentence(first_mani_query, str_rest_data)

        return second_mani_query

    def exec_query(self, query):
        return exec_return_query(query)
