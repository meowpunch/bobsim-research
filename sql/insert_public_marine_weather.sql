INSERT INTO public_marine_weather
(
                `지점`,`일시`,`평균 풍속(m/s)` ,
                `평균 기압(hPa)` ,`평균상대습도(pct)`,
                `평균 기온(°C)`  ,`평균 수온(°C)`  ,
                `평균 최대 파고(m)`  ,`평균 유의 파고(m)`  ,
                `최고 유의 파고(m)`  ,`최고 최대 파고(m)`  ,
                `평균 파주기(sec)`  ,`최고 파주기(sec)` )
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
