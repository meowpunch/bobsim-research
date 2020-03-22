import sys
from functools import reduce

import pandas as pd

from data_pipeline.dtype import dtype
from query_builder.core import InsertBuilder
from util.logging import init_logger
from util.s3_manager.manager import S3Manager


class Processor:
    """
        load public data from s3 origin bucket and check valid
        save to staging schema in rds and return pd DataFrame
    """

    def __init__(self, key):
        self.key = key
        self.logger = init_logger()

        # s3
        self.bucket_name = "production-bobsim"
        self.s3_key = "public_data/{dir}/origin".format(dir=self.key)

        # valid check
        self.dtypes = dtype[key]
        self.columns = list(self.dtypes.keys())

        # rdb
        self.schema_name = "public_data"
        self.table_name = self.key

        # return
        self.df = None

    def load(self):
        """
            init S3Manager instances and fetch objects
        :return: list of pd DataFrame (origin)
        """
        manager = S3Manager(bucket_name=self.bucket_name)
        df_list = manager.fetch_objects(key=self.s3_key)

        self.logger.info("{num} files is loaded".format(num=len(df_list)))
        self.logger.info("load df from origin bucket")
        return df_list

    def validate(self):
        # load the list of DataFrames
        df_list = self.load()

        # combine the list and check DataFrame type.
        def combine(accum, ele):
            """
            :return: pd DataFrame combined with df_list
            """
            tmp = ele[self.columns]
            return accum[self.columns].append(tmp)

        tmp_df = reduce(combine, df_list)
        self.df = tmp_df.astype(dtype=self.dtypes)

    def save(self):
        """
            # TODO: catch error that query_builder raise
                    ask how to handle pymysql.err.OperationalError by input_value size
            save validated data to RDS
        :return: success or fail (bool)
        """
        # replace pd.NA to None in order to save NULL to rds.
        tmp_df = self.df.replace({pd.NA: None}, inplace=False)

        # temporary head(2)
        input_value = tmp_df.head(2).apply(lambda x: tuple(x.values), axis=1)

        qb = InsertBuilder(
            schema_name=self.schema_name,
            table_name=self.table_name,
            value=tuple(input_value) if len(input_value) > 1 else input_value[0]
        )
        qb.execute()

    def execute(self):
        """
        :return: pandas DataFrame that load from s3 origin bucket
        """
        self.logger.info("start processing {key}".format(key=self.key))

        try:
            self.validate()
            self.save()
        except KeyError:
            self.logger.critical("columns are not matched", exc_info=True)
            sys.exit()
        except Exception as e:
            # TODO: change Exception.
            self.logger.critical(e, exc_info=True)
            sys.exit()

        self.logger.info("success processing {key}".format(key=self.key))
        return self.df
