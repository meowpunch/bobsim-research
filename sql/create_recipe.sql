CREATE TABLE recipe ( id INT NOT NULL AUTO_INCREMENT PRIMARY KEY ,
        name VARCHAR(50) NOT NULL,
        season_id INT ,
        maker VARCHAR(190) NOT NULL ,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE only_one_make (name , maker ),
        FOREIGN KEY (season_id) REFERENCES season(id)  ON UPDATE CASCADE ON DELETE CASCADE)