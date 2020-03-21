import pandas as pd

from query_builder.core import QueryBuilder, CreateBuilder, DeleteBuilder, SelectBuilder, UpdateBuilder, InsertBuilder, \
    DropBuilder
from query_builder.create_table import CreateTable
from util.alter import alter_type_list_to_str
from util.db import exec_return_query, show_columns, show_data, load_query, exec_void_query
import json


def migrate():
    table_list = ['item', 'price', 'season', 'recipe', 'recipe_item',
                  'item_frequency', 'user', 'user_item']
    reverse_list = list(reversed(table_list))

    for i in range(0, len(reverse_list)):
        z = DropBuilder(reverse_list[i])
        z.execute()

    for i in range(0, len(table_list)):
        x = CreateBuilder(table_list[i])
        x.execute()

        exec_void_query(load_query('insert_{}_init.sql'.format(table_list[i])))

    for i in range(0, len(table_list) - 1):
        x = SelectBuilder(table_list[i], '*')
        print(x.execute())


def main():
    """
        migrate
        TODO : a few moment later, this sentences convert to func

    """

    InsertBuilder(table_name='temp', )
    InsertBuilder(
        schema_name='public_data',
        table_name='item_price_info',
        value=input_df
    )
    # migrate()


if __name__ == '__main__':
    main()
