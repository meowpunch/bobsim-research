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

    def load(self):
        """
            init S3Manager instances and fetch objects
        :return: list of pd DataFrame (origin)
        """
        manager = S3Manager(bucket_name='production-bobsim')
        df_list = manager.fetch_objects(
            key='public_price/origin/csv'
        )

        self.logger.info("%d files is loaded" % len(df_list))
        self.logger.info("success to load df from origin bucket")
        return df_list

    def validate(self):
        # load the list of DataFrames
        df_list = self.load()

        # combine the list and check DataFrame type.
        def combine(accum, ele):
            """
            :return: pd DataFrame combined with df_list
            """
            tmp = ele[self.columns].astype(dtype=self.dtypes, copy=True)
            return accum[self.columns].astype(dtype=self.dtypes).append(tmp)
        tmp_df = reduce(combine, df_list)
        self.df = tmp_df.replace({pd.NA: None})

    def save(self):
        """
            # TODO: catch error that query_builder raise
                    ask how to handle pymysql.err.OperationalError by input_value size
            save validated data to RDS
        :return: success or fail (bool)
        """
        input_value = self.df.head(2).apply(lambda x: tuple(x.values), axis=1)

        qb = InsertBuilder(
            schema_name='public_data',
            table_name='item_price_info',
            value=tuple(input_value)
        )
        qb.execute()

        # temporary true
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
