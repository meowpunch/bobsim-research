from util.db import load_return_query


class Simulator:
    """
        user_num -> Assume the number of Active User

    """
    def __init__(self):
        self.sql_filename = 'item_distribution.sql'
        self.user_num = 1000

    def execute(self):
        return self.process()

    def load_data(self):
        return load_return_query(self.sql_filename)

    def process(self):
        raw_data = self.load_data()
        print(type(raw_data))
        for row in raw_data:
            print(row)
        """
        TODO:
            0. set the number of virtual active user
           ----
            1. quantify food materials
            2. price food materials
            3. find likely menu(recipe) by user's food materials.
            4. calculate object function (opportunity cost)
        """
        print(raw_data)


