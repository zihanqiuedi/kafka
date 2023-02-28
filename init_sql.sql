CREATE DATABASE IF NOT EXISTS stock_price default charset utf8 collate utf8_general_ci;
DROP TABLE IF EXISTS  stock_price.history_price;
CREATE TABLE stock_price.history_price (
	pk_date varchar(200) NOT NULL COMMENT 'Date',
	company_name varchar(200) NOT NULL COMMENT 'Company_Name',
	price  DOUBLE COMMENT 'Stock_Price'
) DEFAULT CHARACTER SET=utf8mb4 COMMENT='Historical Stock Price';