import datetime
import json

from simulator.price import price
from simulator.quantity import quantify
from simulator.user import User
from util.db import load_query, exec_return_query
from util.s3 import save_json


class Simulator:
    """
        Trigger: Airflow Scheduler (call one time per one day)
        Running Where: ECS?

        Considering the cost & efficiency,
        we need to decide one user per one call or multi user per one call

        multi
            user_num -> How many active user is simulated from one call

        solo (Assumed)
    """

    def __init__(self):

        self.sql_filename = 'item_distribution.sql'
        # raw data form
        self.json_filename = 'form.json'
        self.dict_data = self.load_data_form()
        self.type = 0

        # self.user_num = 1

    def execute(self):
        return self.process()

    def load_data_form(self):
        with open(self.json_filename, "r") as raw_data:
            return json.load(raw_data)

    def load_data(self):
        query = load_query(self.sql_filename)
        return exec_return_query(query)

    def process(self):
        """
        TODO:
            Process logic
            1. create or select virtual user.
                if there is already id, retrieve user.
            2. set user's behavior type and play (check comments in User class)
            3. generate data ( feature? )
            4. save raw_data(json) to S3

        :return:
        """
        self.play_user()
        self.generate_data()
        # self.save_raw_data(self.dict_data)

    def generate_data(self):
        """
        TODO:
            by user's behavior
            type 0
            1. just user profile
            type 1
            1. load data (join query)
            2. quantify
            3. price
        :return: raw data
        """
        total_data = self.load_data()
        partial_data = total_data.head(10)
        print("partial data (2) \n", partial_data)

        q_data = partial_data.item_frequency\
            .map(lambda x: quantify(
                num=1,
                freq=x,
                d_type=0
            ))
        # for checking
        print(q_data)

        p_data = partial_data.apply(lambda x: price(
            num=1,
            avg=x.average,
            delta=x.delta,
            d_type=x.distr_type
        ), axis=1)
        # for checking
        print(p_data)

        # tmp_check_data
        pass

    @staticmethod
    def play_user():
        """
        TODO: user's behavior process not like below sequentially.
        :return:
        """

        print("-----playing user-----")
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
