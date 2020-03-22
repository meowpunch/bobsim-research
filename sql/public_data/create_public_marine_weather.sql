CREATE TABLE public_marine_weather (
        `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        `지점` INT NOT NULL,
        `일시` DATE NOT NULL ,
        `평균 풍속(m/s)` DECIMAL(3,1),
        `평균 기압(hPa)` DECIMAL(5,1),
        `평균상대습도(pct)` INT,
        `평균 기온(°C)` DECIMAL(3,1),
        `평균 수온(°C)` DECIMAL(3,1),
        `평균 최대 파고(m)` DECIMAL(3,1),
        `평균 유의 파고(m)` DECIMAL(3,1),
        `최고 유의 파고(m)` DECIMAL(3,1),
        `최고 최대 파고(m)` DECIMAL(3,1),
        `평균 파주기(sec)` DECIMAL(3,1),
        `최고 파주기(sec)` DECIMAL(4,1)
        CONSTRAINT unique_per_day UNIQUE(지점, 지점명)
)