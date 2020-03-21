import sys
from functools import reduce

import pandas as pd

from data_pipeline.dtype import public_price
from data_pipeline.public_price.loader import Loader
import logging

from query_builder.core import InsertBuilder
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
        """
        # load df list and check types
        df_list = self.load()

        self.logger.debug(df_list[0].columns.tolist())
        self.logger.debug(self.columns)

        def combine(accum, ele):
            tmp = ele[self.columns].astype(dtype=self.dtypes, copy=True)
            return accum[self.columns].astype(dtype=self.dtypes).append(tmp)

        self.df = reduce(combine, df_list)

        self.logger.debug(self.df)
        self.logger.debug(self.df.dtypes)

    def save(self):
        """
            save validated data to RDS
        :return: success or fail (bool)
        """
        tmp = self.df.apply(lambda x: tuple(x.values), axis=1)
        input_df = ', '.join(map(str, tmp))
        qb = InsertBuilder(
            schema_name='public_data',
            table_name='item_price_info',
            value=input_df
        )
        # qb.execute()
#         tmp_qb = InsertBuilder(schema_name='public_data',
#                                table_name='item_price_info'
#                                , value=('2017-01-02',7,'소비자가격'	101	벼	111	쌀	10101	일반계	1	일반계	1	상(1등급)			20KG	36300	36300	1102	서울서부	4002513	경동시장	12	경동시장
# ))
        return True

    def execute(self):
        try:
            self.validate()
        except KeyError:
            self.logger.critical("columns are not matched", exc_info=True)
            sys.exit()

        s = self.save()

        if s:
            self.logger.info("")
            return self.df
        else:
            self.logger.critical("save fail")
            sys.exit()
