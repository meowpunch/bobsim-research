import datetime

from simulator.quantity import quantify
from simulator.user import User
from util.db import load_query, exec_return_query
from util.s3 import save_json


class Simulator:
    """
        Trigger: Airflow Scheduler
        Running Where: ECS?

        Considering the cost & efficiency,
        we need to decide one user per one call or multi user per one call

        multi
            user_num -> How many active user is simulated from one call

        solo (Default)
    """

    def __init__(self):
        self.sql_filename = 'item_distribution.sql'
        # for test
        self.dict_data = {
            "first": "solo",
            "second": [
                "arr0",
                "arr1"
            ],
            "third": {
                "nested": {
                    "num": -1
                }
            }
        }
        # self.user_num = 1

    def execute(self):
        return self.process()

    def load_data(self):
        query = load_query(self.sql_filename)
        return exec_return_query(query)

    def process(self):
        """
        TODO:
            1. fabricate and get data
            2. play virtual user.
            3. save S3
            4. visualize
        :return:
        """
        self.fabricate_data()
        self.play_user()
        # self.save_raw_data(self.dict_data)

    @staticmethod
    def printing(x):
        return print(x)

    def fabricate_data(self):
        """
        TODO: load data & make distribution
        :return: raw data
        """

        total_data = self.load_data()
        partial_data = total_data.head(2)

        print(partial_data)
        partial_data['item_frequency'].map(lambda x : quantify(num=1, freq=x, d_type=0))

        for partial_data[['average', 'delta', 'distr_type']].iter
        # map(self.printing, pd['name'])
        # tmp_check_data
        pass

    @staticmethod
    def play_user():
        """
        TODO: user's behavior process not like below sequentially.
        :return:
        """
        vu = User()  # virtual user
        vu.login()
        vu.search_menu()

    @staticmethod
    def save_raw_data(dict_data):
        """
            TODO: save raw_data
        :return: false or true
        """
        now = datetime.datetime.now()
        filename = str(now.day) + str(now.month) + str(now.year)
        save_json(directory="recommender/raw-data", filename=filename, data=dict_data)


