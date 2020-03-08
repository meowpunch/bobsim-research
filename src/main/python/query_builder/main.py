from query_builder.core import QueryBuilder
from query_builder.create_table import CreateTable


def main():
    create_query_builder = CreateTable('item')
    create_query_builder.execute()


if __name__ == '__main__':
    main()
