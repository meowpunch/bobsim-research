dtype = {
    "raw_material_price": {
        "조사일자": "object",
        "조사구분코드": "UInt8",
        "조사구분명": "object",
        "표준품목코드": "UInt16",
        "표준품목명": "object",
        "조사가격품목코드": "UInt16",
        "조사가격품목명": "object",
        "표준품종코드": "object",
        "표준품종명": "object",
        "조사가격품종코드": "UInt8",
        "조사가격품종명": "object",
        "조사등급코드": "UInt8",
        "조사등급명": "object",
        "표준단위코드": "UInt8",
        "표준단위명": "object",
        "조사단위명": "object",
        # TODO: type casting error btw UInt and float while aggregate mean
        "당일조사가격": "int",
        "전일조사가격": "UInt32",
        "조사지역코드": "UInt16",
        "조사지역명": "object",
        "표준시장코드": "UInt32",
        "표준시장명": "object",
        "조사가격시장코드": "UInt8",
        "조사가격시장명": "object"
    },

    # TODO: Find reasons not applicable
    "terrestrial_weather": {
        "지점": "int8",
        "지점명": "object",
        "일시": "object",
        "평균기온(°C)": "float16",
        "최저기온(°C)": "float16",
        "최고기온(°C)": "float16",
        "강수 계속시간(hr)": "float16",
        "일강수량(mm)": "float16",
        "최대 풍속(m/s)": "float16",
        "평균 풍속(m/s)": "float16",
        "최소 상대습도(pct)": "float16",
        "평균 상대습도(pct)": "float16",
        "합계 일조시간(hr)": "float16",
        "합계 일사량(MJ/m2)": "float16"
    },

    "marine_weather": {
        "지점": "int16",
        "일시": "object",
        "평균 풍속(m/s)": "float16",
        "평균기압(hPa)": "float16",
        "평균 상대습도(pct)": "float16",
        "평균 기온(°C)": "float16",
        "평균 수온(°C)": "float16",
        "평균 최대 파고(m)": "float16",
        "평균 유의 파고(m)": "float16",
        "최고 유의 파고(m)": "float16",
        "최고 최대 파고(m)": "float16",
        "평균 파주기(sec)": "float16",
        "최고 파주기(sec)": "float16",
    }
}
