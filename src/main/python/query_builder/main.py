from query_builder.core import QueryBuilder, CreateBuilder, InsertBuilder, DeleteBuilder, SelectBuilder
from query_builder.create_table import CreateTable
from util.db import exec_return_query


def main():
    select_query_builder = SelectBuilder('*', 'item', 'WHERE id!=1', 'GROUP BY sensitivity', 'LIMIT 5')
    print(select_query_builder.execute())


if __name__ == '__main__':

    main()


"""

    # dict = {"TABLE": 'item', "ATT_NAME": 'id', "INSERT_VALUE": (1, '경재'), "a":1}
    # print(type(dict.items()))
    # iterator = list(dict.items())[2:])
    # a = list(map(lambda x: func(x), iterator)
    # print(a)
    
 self.init_dict = {"TABLE_NAME": None,
                          "ATT_NAME": None,
                          "INPUT_VALUE": None,
                          "WHERE": None,
                          "GROUP_BY": None,
                          "ORDER_BY": None,
                          "HAVING": None,
                          "LIMIT": None,
                          "UPDATE_VALUE": None,
                          "OFFSET": None}"""

