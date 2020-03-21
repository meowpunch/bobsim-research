import sys
from functools import reduce

import pandas as pd

from data_pipeline.dtype import public_price
from data_pipeline.public_price.loader import Loader
import logging

from util.logging import init_logger
from util.s3_manager.manager import S3Manager


class Processor:
    """
        origin bucket (s3) -> process bucket (rds_public_data, staging schema)
    """

    def __init__(self):
        self.logger = init_logger()
        self.dtypes = public_price
        self.columns = list(self.dtypes.keys())

        self.df = None

    # @staticmethod
    def load(self):
        """

        :return: list of pd DataFrame (origin)
        """
        # init S3Manager instances and fetch objects
        manager = S3Manager(bucket_name='production-bobsim')
        df_list = manager.fetch_objects(
            key='public_price/origin/csv'
        )

        self.logger.debug("%d files is loaded" % len(df_list))
        self.logger.info("success to load df from origin bucket")
        return df_list

    def validate(self):
        """
            validate data in origin bucket
            1. how to handling null_value

            # SAVE RDS -> Auto type checking??
        :return: validity (bool)
        """
        # load df list and check types
        df_list = self.load()

        def func(x):

            tmp_df = x.astype(dtype=self.dtypes)

            tmp2_df = tmp_df[self.columns]

            self.df.append(tmp2_df)

            return True

        self.logger.debug(df_list[0].columns.tolist())
        self.logger.debug(self.columns)
        # invalid = list(filter(
        #     lambda x: x.columns.values.tolist() != self.columns,
        #     df_list
        # ))
        # if len(invalid) == 0:
        #     raise Exception("column is not matched")

        def combine(accum, ele):
            tmp = ele[self.columns].astype(dtype=self.dtypes, copy=True)
            return accum[self.columns].astype(dtype=self.dtypes).append(tmp)

        self.df = reduce(combine, df_list)

        self.logger.debug(self.df)
        self.logger.debug(self.df.dtypes)
        return False


    @staticmethod
    def save(self):
        """
            save validated data to RDS
        :return: success or fail (bool)
        """
        return False

    def execute(self):
        try:
            b = self.validate()
        except KeyError:
            self.logger.critical("columns are not matched", exc_info=True)
            sys.exit()

        if b:
            s = self.save()
        else:
            self.logger.critical("validate fail")
            sys.exit()

        if s:
            return self.df
        else:
            self.logger.critical("save fail")
            sys.exit()
