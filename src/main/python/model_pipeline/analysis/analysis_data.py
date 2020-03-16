import pandas as pd

from util.general import load_csv_pandas

data_df = load_csv_pandas('원천조사가격정보_201601.csv')
print(type(data_df))

data_location = data_df['조사지역명']
print(type(data_location))
value_counts = data_location.vue_counts()


# print(value_counts.head(20))
location_metropolitan = ['서울서부', '대전', '부산', '대구', '광주', '인천', '울산', '제주']


def contain_metropolitan(data_location_series, location_list):
    for x in location_list :
        y = data_location_series.str.contains(x, regex=False)
        print(x, ' : ', y.sum())

# 카테고리 별[][][]로 null값이 몇개인가 , 카테고리가 몇개가 있냐?

data_ssal_df = data_df[['조사가격품목명', '표준품종명'=='일반계', '조사가격품종명'=='일반계']]
data_ssal_location = data_ssal_df['조사지역명']
ssal_value_counts = data_ssal_location.value_counts()
print(ssal_value_counts)
