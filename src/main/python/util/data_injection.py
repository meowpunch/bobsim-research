import urllib.request
import json
import pandas as pd
from pandas.io.json import json_normalize
import requests
import datetime

#-*- coding:utf-8 -*-

"""
def main():
    date = '20200420'
    service_key = ''
    url='http://apis.data.go.kr/B552895/openapi/service/OrgPriceExaminService/getExaminPriceList?\
    ServiceKey='+service_key+'&type=json&pageNo=1&numOfRows=10&examinDe='+date+'&examinCd=6&prdlstCd=223'

    response = urllib.request.urlopen(url)

    json_str = response.read().decode("utf-8")

    json_object = json.loads(json_str)
    # print(json_object)
    # body = [json_object['body']['item']]

    df = json_normalize(json_object['body']['item'])
    print(df)
"""
def main():
    today = datetime.date.today()
    start_dt = today - datetime.timedelta(days=100)
    today = today.strftime('%Y%m%d')
    start_dt = start_dt.strftime('%Y%m%d')

    args= {
        'numberOfRows': 10,
        'pageNo': 1,
        'ServiceKey': "",
        'examinDe': today,
    }

    args_str = ""
    for k, v in args.items():
        args_str += '%s=%s' % (k, v)

    res = requests.get('http://apis.data.go.kr/B552895/openapi/service/OrgPriceExaminService/getExaminPriceList?returnType=json%s' % args_str )
    data = res.json()
    items = data['response']['body']['items']
    df_items = pd.DataFrame(items)
    df_csv = df_items.to_csv('json.csv', encoding='utf-8-sig')
    print(items)

    # TODO : save df_csv to s3


if __name__ == '__main__':
    main()