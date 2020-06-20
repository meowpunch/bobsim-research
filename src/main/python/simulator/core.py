import datetime
import json

import pandas as pd

from query_builder.core import SelectBuilder
from simulator.menu import cost_menu
from simulator.price import price
from simulator.quantity import quantify
from simulator.user import User
from utils.db import load_query, exec_return_query
from utils.s3_manager import save_json


class Simulator:
    """
        Trigger: Airflow Scheduler (call one time per one day)
        Running Where: ECS?

        Considering the cost & efficiency,
        we need to decide one user per one call or multi user per one call

        multi (necessary)
            user_num -> How many active user is simulated from one call

        solo (temporary)
    """

    def __init__(self):
        self.sql_filename = 'item_distribution.sql'
        # TODO: for multi user, this should be changed
        self.user_num = 4
        self.user = User(self.user_num)
        self.timestamp = datetime.datetime.now()

    def execute(self):
        return self.process()

    def process(self):
        """
        TODO:
            Process logic
            1. create or select virtual user. (if there is already id, retrieve user.)
            2. set user's behavior type and play (check comments in User class)
            3. generate extra data ( feature? )
            4. save raw_data(json) to S3

        :return:
        """
        self.user = User(self.user_num)
        self.user.capture_fridge(self.fridge_image, self.probable_menus)

        raw_data = self.raw_data_dic()
        # self.save_raw_data(raw_data)

    def load_data(self):
        query = load_query(self.sql_filename)
        return exec_return_query(query)

    def get_timestamp(self):
        self.timestamp = datetime.datetime.now()
        return self.timestamp

    def fridge_image(self):
        """
        GENERATE VIRTUAL IMAGE OF USER'S FRIDGE
            TODO:
                1. load data and filter dirty data.
                2. quantify
                3. price
                4. timestamp
        :return: fridge image
        """
        td = self.load_data()
        mask = td.apply(lambda x:
                        x.average is not 0 and
                        x.delta is not 0, axis=1)
        fd = td[mask]

        qd = quantify(fd, self.user_num)
        print(qd)
        fridge = price(qd, self.user_num)
        print(fridge)

        self.get_timestamp()
        return fridge

    def probable_menus(self, fridge):
        """
            TODO: find probables menu for each user.
        :param fridge: group's total fridge
        :return: list of probable menu(DataFrame) for each user.
        """
        # user_list = list(range(self.user_num))
        #
        # def func(user_id):
        #     cost_menu()
        #     pass
        #
        # m = list(map(func, user_list))
        # print(m[0])

        menu = cost_menu(fridge, self.user_num)
        print(menu)
        pass

    def raw_data_dic(self):
        """
            TODO: If feature is more complex, structure maybe changed.

            raw_data and record maybe point same space.
        """
        raw_data = self.user.record
        f_fridge = self.user.fridge.drop(['id'], axis=1)
        f_menu = self.user.menu.drop(['id'], axis=1)
        raw_data["TIMESTAMP"] = str(self.timestamp)
        raw_data["driven"] = self.user.b_type
        raw_data["ingredients"] = f_fridge.to_dict('records')
        raw_data["menu"] = f_menu.to_dict('records')

        # for checking
        print("\n-----raw data------")
        print(raw_data)
        return raw_data

    @staticmethod
    def play_user():
        """
        TODO: by user behavior pattern, execute various routines
        :return:
        """
        pass

    def save_raw_data(self, dict_data):
        """
            1. stamp time
            2. save raw_data
        :return: false or true
        """
        now = datetime.datetime.now()
        filename = str(now.day) + str(now.month) + str(now.year)
        save_json(directory="recommender/raw-data/" + str(self.user.b_type), filename=filename, data=dict_data)