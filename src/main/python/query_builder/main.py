from query_builder.core import QueryBuilder, CreateBuilder, InsertBuilder, DeleteBuilder, SelectBuilder, UpdateBuilder
from query_builder.create_table import CreateTable
from util.db import exec_return_query


def main():
    # select_query_builder = SelectBuilder('*', 'item', 'WHERE id!=1', 'GROUP BY sensitivity', 'LIMIT 5')
    # print(select_query_builder.execute())
    #
    update_query_builder = UpdateBuilder('temptemptemp', update_value="sensitivity=0", where_clause="WHERE name= '진훈'")
    update_query_builder.execute()


if __name__ == '__main__':
    main()

"""

    def manipulate(self, query):  # extract from where_clause to offset
        # dict -> list
        rest_data = alter_type_dict_to_list(self.init_dict, 2, len(self.init_dict))
        # add att,table name to 'SELECT {} FROM {}' sql file
        first_mani_query = query % (self.init_dict["ATT_NAME"], self.init_dict["TABLE_NAME"])
        # remove None
        clean_data = remove_none(rest_data)
        # list -> str
        str_rest_data = alter_type_list_to_str(clean_data)
        # add blank between 'SELECT {} FROM {}' and 'rest data'
        second_mani_query = first_mani_query + ' ' + str_rest_data
 """
