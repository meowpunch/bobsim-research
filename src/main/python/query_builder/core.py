from query_builder.create_table import CreateTable
from util.db import load_query
from util.s3 import list_bucket_contents


class QueryBuilder:

    def __init__(self):
        self.sql_filename = 'create_item.sql'

    def execute(self):
        return self.process()

    def load_data(self):
        return load_query(self.sql_filename)

    def process(self) :

        a = CreateTable(self.sql_filename)

        """
        TODO: logic comes here
        """
