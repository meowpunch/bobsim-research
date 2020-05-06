import urllib.request
import json

import boto3
import pandas as pd
from pandas.io.json import json_normalize
import requests
import datetime
from functools import reduce

#-*- coding:utf-8 -*-

from util.s3 import save_to_s3
from util.s3_manager.manage import S3Manager


class data_injection():
    def __init__(self):
        self.today = datetime.date.today()
        self.s3 = boto3.resource('s3')
        self.s3_bucket = self.s3.Bucket("production-bobsim")
        self.bucket_name="production-bobsim"
        self.code_list= [256, 257, 512, 515, 514, 516, 644, 646, 641,
                         640, 642, 511, 652, 141, 142, 143, 144, 639,
                         151, 152, 535, 411, 412, 413, 414, 415, 416,
                         418, 419, 420, 421, 422, 423, 424, 425, 426,
                         427, 428, 312, 313, 314, 315, 316, 317, 318,
                         319, 211, 212, 213, 214, 215, 216, 221, 222,
                         223, 224, 225, 226, 611, 248, 613, 231, 232,
                         233, 615, 619, 111, 112, 241, 242, 243, 628,
                         245, 246, 247, 625, 252, 253, 638, 255]
        self.args = {
            #'numberOfRows': 10,
            #'pageNo': 1,
            #'ServiceKey': "7c785c42110451cba1eeb8b572111a4c48b98cba8d49c92fdb801607727df47c",
            'EXAMIN_DE':"20200504", #self.today,
            "&EXAMIN_PRDLST_CODE": 640
        }
        # manager = S3Manager(bucket_name="production-bobsim")

    def call_price(self):
        # args_str = ""
        #
        # for k, v in self.args.items():
        #     args_str += '%s=%s' % (k, v)
        #
        # res = requests.get(
        #     'http://211.237.50.150:7080/openapi/7c785c42110451cba1eeb8b572111a4c48b98cba8d49c92fdb801607727df47c/json/Grid_20151128000000000315_1/1/5?{arg}'.format(arg=args_str))
        # print('http://211.237.50.150:7080/openapi/7c785c42110451cba1eeb8b572111a4c48b98cba8d49c92fdb801607727df47c/json/Grid_20151128000000000315_1/1/5?{arg}'.format(arg=args_str))
        # print(args_str)
        # data = res.json()
        # items = data["Grid_20151128000000000315_1"]["row"]
        # print(items)
        # df_items = pd.DataFrame(items)
        # df_items.drop(["ROW_NUM"], axis=1, inplace=True)
        # print(df_items)
        # TODO: remove #
        # load_key = "public_data/open_data_raw_material_price/origin/csv/{filename}.csv".format(filename="202005040") #self.today)
        # manager = S3Manager(bucket_name="production-bobsim")
        # manager.save_df_to_csv(df=df_items, key=load_key)

        def func(x: int):
            a = {
                'EXAMIN_DE' : "20200504", #self.today,
                "&EXAMIN_PRDLST_CODE": x
            }
            args_str = ""
            for k, v in a.items():
                args_str += '%s=%s' % (k, v)
            res = requests.get('http://211.237.50.150:7080/openapi/7c785c42110451cba1eeb8b572111a4c48b98cba8d49c92fdb801607727df47c/json/Grid_20151128000000000315_1/1/5?{arg}'.format(arg=args_str))
            data = res.json()
            print(data)
            # if len(data['Grid_20151128000000000315_1']['row']) is not 0:
            items = data["Grid_20151128000000000315_1"]["row"]
            return pd.DataFrame(items)

        df_list = list(map(lambda x: func(x), self.code_list))
        print(df_list)

        def concat(x, y):
            if x.empty:
                return x
            elif y.empty:
                return x
            else:
                return pd.concat([x, y])

        full_df = reduce(lambda x, y: concat(x, y), df_list)
        full_df.drop(["ROW_NUM"], axis=1, inplace=True)
        print(full_df)
        load_key = "public_data/open_data_raw_material_price/origin/csv/{filename}.csv".format(filename="20205040") #self.today)
        manager = S3Manager(bucket_name="production-bobsim")
        manager.save_df_to_csv(df=full_df, key=load_key)

        # df_csv = df_items.to_csv('json.csv', encoding='utf-8-sig')




def main():
    a = data_injection()
    a.call_price()

if __name__ == '__main__':
    main()