CREATE TABLE public_terrestrial_weather (
        `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY ,
        `지점` INT NOT NULL,
        `지점명` VARCHAR(50) NOT NULL,
        `일시` DATE NOT NULL,
        `평균기온(°C)` DECIMAL(5,2),
        `최저기온(°C)` DECIMAL(5,2),
        `최고기온(°C)` DECIMAL(5,2),
        `강수 계속시간(hr)` DECIMAL(5,2),
        `일강수량(mm)` DECIMAL(5,2),
        `최대 풍속(m/s)` DECIMAL(5,2),
        `평균 풍속(m/s)` DECIMAL(5,2),
        `최소 상대습도(pct)` DECIMAL(5,2),
        `평균 상대습도(pct)` DECIMAL(5,2),
        `합계 일조시간(hr)` DECIMAL(5,2),
        `합계 일사량(MJ/m2)` DECIMAL(5,2),

        CONSTRAINT unique_code UNIQUE (지점, 지점명, 일시)
)