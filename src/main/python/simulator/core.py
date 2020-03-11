import datetime
import json

import pandas as pd

from query_builder.core import SelectBuilder
from simulator.menu import cost_menu
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
            3. generate extra data ( feature? )
            4. save raw_data(json) to S3

        :return:
        """
        vu = User(1)

        v_fridge = vu.capture_fridge(self.fridge_image)

        self.generate_extra(fridge_image=v_fridge)
        # self.play_user()
        # self.generate_data()

        # self.save_raw_data(self.dict_data)

    def fridge_image(self):
        """
        GENERATE VIRTUAL IMAGE OF USER'S FRIDGE
            TODO:
                1. load data and filter dirty data.
                2. quantify
                3. price
        :return: fridge image
        """
        td = self.load_data()
        mask = td.apply(lambda x:
                        x.average is not 0 and
                        x.delta is not 0, axis=1)
        fd = td[mask]

        qd = quantify(fd)

        fridge = price(qd)

        return fridge

    @staticmethod
    def generate_extra(fridge_image):
        """
        TODO:
            ...
            type 2:
            find menu and cal cost
            ...

        :return: raw data
        """
        menu_cost = cost_menu(fridge=fridge_image)
        print(menu_cost)
        return menu_cost

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
