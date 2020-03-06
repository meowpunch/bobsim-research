CREATE TABLE IF NOT EXISTS price ( id INT NOT NULL AUTO_INCREMENT PRIMARY KEY ,
        item_id INT ,
        average INT NOT NULL ,
        delta INT NOT NULL ,
        distr_type TINYINT(2) NOT NULL ,
        quantity_per_one NUMERIC(3,2) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        FOREIGN KEY (item_id) REFERENCES item(id)
        ON UPDATE CASCADE ON DELETE CASCADE);