dtype = {
    "raw_material_price": {
        "조사일자": "datetime64",
        "조사지역명": "object", "조사단위명": "object",
        "조사등급명": "object", "조사구분명": "object",
        "표준품목명": "object", "조사가격품목명": "object", "표준품종명": "object", "조사가격품종명": "object",
        # TODO: type casting error btw UInt and float while aggregate mean
        "당일조사가격": "int",
    },

    # TODO: Find reasons not applicable
    "terrestrial_weather": {
        "일시": "datetime64",
        "평균기온(°C)": "float32",
        "최저기온(°C)": "float32",
        "최고기온(°C)": "float16",
        "강수 계속시간(hr)": "float16",
        "일강수량(mm)": "float16",
        "최대 풍속(m/s)": "float16",
        "평균 풍속(m/s)": "float16",
        "최소 상대습도(pct)": "float16",
        "평균 상대습도(pct)": "float32"
    },

    "marine_weather": {
        "일시": "datetime64",
        "평균 풍속(m/s)": "float16",
        "평균기압(hPa)": "float32",
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
