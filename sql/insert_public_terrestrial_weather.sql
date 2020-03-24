INSERT IGNORE INTO public_terrestrial_weather
(
        `지점`, `지점명`,`일시`, `평균기온(°C)`,
        `최저기온(°C)`, `최고기온(°C)`, `강수 계속시간(hr)`,
        `일강수량(mm)`, `최대 풍속(m/s)`, `평균 풍속(m/s)`,
        `최소 상대습도(pct)`, `평균 상대습도(pct)`,
        `합계 일조시간(hr)`, `합계 일사량(MJ/m2)`
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)