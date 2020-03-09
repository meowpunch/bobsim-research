from query_builder.core import QueryBuilder, CreateBuilder, InsertBuilder
from query_builder.create_table import CreateTable


def main():
    create_query_builder = InsertBuilder('item', ('고고고씽', 1))
    create_query_builder.execute()

if __name__ == '__main__':

    main()

