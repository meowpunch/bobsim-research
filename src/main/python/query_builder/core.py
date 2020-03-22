from util.alter import *
from util.db import exec_return_query, exec_void_query, load_query
from util.s3 import list_bucket_contents

from abc import *


class QueryBuilder(metaclass=ABCMeta):

    # def __init__(self, table_name):
    def __init__(self, schema_name='bobsim_schema', table_name=None, att_name=None,
                 value=None, where_clause: str = None,
                 group_by=None,
                 having=None, order_by: str = None,
                 limit: str = None, offset: int = None):
        self.init_dict = {"TABLE_NAME": table_name,
                          "ATT_NAME": att_name,
                          "VALUE": value,
                          "WHERE": where_clause,
                          "GROUP_BY": group_by,
                          "HAVING": having,
                          "ORDER_BY": order_by,
                          "LIMIT": limit,
                          "OFFSET": offset,
                          }
        """
        TODO :  divide init_dict into 2~4 part 
                and have to more functional programming
                ( init_dict , where_condition, dict, join etc..)
        
        """
        self.schema_name = schema_name
        self.query = None

    def store_query(self, sql_filename):
        self.query = load_query(sql_filename)

    @abstractmethod
    def exec_query(self, query):
        pass


class VoidQueryBuilder(QueryBuilder):

    def exec_query(self, query, args):
        exec_void_query(args=args, query=query, schema_name=self.schema_name)

    def check_query(self):
        pass


class ReturnQueryBuilder(QueryBuilder):

    def exec_query(self, query):
        exec_return_query(query=query, schema_name=self.schema_name)


# QueryBuilder-VoidQueryBuilder-CreateBuilder
class CreateBuilder(VoidQueryBuilder):

    def execute(self):
        self.process()

    def process(self):
        #  1. Store sql 2. Exec stored sql 3. check

        self.store_query('create_{}.sql'.format(self.init_dict["TABLE_NAME"]))
        self.exec_query(self.query)

        self.check_create()

    def check_create(self):
        # sql = "SHOW FULL COLUMNS FROM %s " % self.init_dict["TABLE_NAME"]

        check_select_builder = SelectBuilder(self.init_dict["TABLE_NAME"], ' * ')

        print(check_select_builder.execute())


# QueryBuilder-VoidQueryBuilder-InsertBuilder
class InsertBuilder(VoidQueryBuilder):
    # InsertBuilder('table_name', 'input_data')

    def execute(self):
        self.process()

    def process(self):
        #  1. Store sql 2. manipulate sql 3. exec sql 4. check

        self.store_query('insert_{}.sql'.format(self.init_dict["TABLE_NAME"]))  # 1

        # query = self.manipulate(self.query)  # 2

        self.exec_query(args=self.init_dict["VALUE"], query=self.query)

        # self.check_insert()

    def check_insert(self):
        # TODO: error for table that has no id(PK)
        a = SelectBuilder(
            schema_name=self.schema_name,
            table_name=self.init_dict["TABLE_NAME"],
            att_name=' * '
            # order_by='ORDER BY id DESC',
            # limit='LIMIT 1'
        )
        print(a.execute())

    def manipulate(self, query):
        # let's think about lots of input_values
        mani_query = query.format(self.init_dict["VALUE"])
        return mani_query


# QueryBuilder-VoidQueryBuilder-InsertBuilder
class DeleteBuilder(VoidQueryBuilder):
    # value = (v1,v2,....,vn) tuple type
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
                                               where_clause=self.init_dict["WHERE"] + " AND " + self.init_dict["VALUE"])
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


class DropBuilder(VoidQueryBuilder):

    def execute(self):
        self.process()

    def process(self):
        self.store_query('drop.sql')  # 1

        mani_query = self.query % self.init_dict["TABLE_NAME"]

        exec_void_query(mani_query)
