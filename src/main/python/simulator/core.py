from util.db import load_query


class Simulator:

    def __init__(self):
        self.sql_filename = 'item_distribution.sql'

    def execute(self):
        return self.process()

    def load_data(self):
        return load_query(self.sql_filename)

    def process(self):
        raw_data = self.load_data()
        """
        TODO: logic comes here
        """
        print(raw_data)


