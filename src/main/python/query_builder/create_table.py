

from util.db import load_query, load_query
from util.s3 import list_bucket_contents

"""
TODO : for using load_query function 
        add path :(create_) at destination_path
        predicted result : destination_path = 'sql/creat_'
        
"""
class CreateTable:

    """
            TODO: logic comes here
            total:  0. get name for create table
                    1. sql store into sql filename
                    2. execute query ( sql)
                    3. throw (select or desc) query (created table)
                    4. return 1 : if raw_data exist
                       return 0 : if fqw_data non-exist

    """

    def __init__(self, name):
        # get create & select table name
        self.create_sql_filename = 'create_%s.sql' % name
        print(self.create_sql_filename)

    def execute(self):
        return self.process()

    def execute_create(self):
        return load_query(self.create_sql_filename)

    def process(self):
       self.execute_create()
       """
       TODO : after executing , check desc create_sql_filename
       """


    # def load_data(self):
    #     return load_query(self.sql_filename)
    #
    # def process(self):
    #     raw_data = self.load_data()


