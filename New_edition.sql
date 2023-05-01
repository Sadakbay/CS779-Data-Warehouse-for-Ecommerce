-- Creating Dimensional tables for Data Warehouse
-- Creating Date Dimensional table with monthly grain
CREATE TABLE DimDate2 (
    date_id SERIAL PRIMARY KEY,
    year INT,
    quarter INT,
    month INT
);

-- Creating Date dimensional table with daily grain
CREATE TABLE DimDate (
    date_id SERIAL PRIMARY KEY,
	full_date TIMESTAMP,
    year INT,
    quarter INT,
    month INT,
    day INT
);

-- Creating Customers dimensional table
CREATE TABLE DimCustomers (
    customer_id varchar(50) NOT NULL PRIMARY KEY,
    customer_unique_id varchar(50) NOT NULL,
    customer_zip_code_prefix INT,
    customer_city VARCHAR(50),
    customer_state VARCHAR(50)
);

-- Creating Sellers dimensional table
CREATE TABLE DimSellers (
    seller_id varchar(50) NOT NULL PRIMARY KEY,
    seller_zip_code_prefix varchar(50) NOT NULL,
    seller_city VARCHAR(50),
    seller_state VARCHAR(50)
);
drop table dimproducts
-- Creating Products dimensional table with SCD type 2
CREATE TABLE DimProducts (
	dim_product_id SERIAL PRIMARY KEY
    product_id varchar(50) NOT NULL,
    product_category_name_english varchar(50),
    product_photos_qty INT,
    product_weight_g INT,
    product_length_cm INT,
    product_height_cm INT,
    product_width_cm INT,
	start_date date, 
	end_date date,
	iscurrent varchar(1),
	CONSTRAINT dimproducts_iscurrent CHECK (iscurrent IN ('Y', 'N'))
);

-- Creating Geolocation dimensional table
CREATE TABLE DimGeolocation (
    geolocation_id SERIAL PRIMARY KEY,
    geolocation_zip_code_prefix INT,
    geolocation_lat DECIMAL,
    geolocation_lng DECIMAL,
    geolocation_city VARCHAR,
    geolocation_state VARCHAR
);

-- Creating fact tables for DW
-- Creating fact review table
CREATE TABLE fact_review (
	fact_review_id int PRIMARY KEY,
	customer_id int,
	avg_review_score int,
	time_to_fill_review decimal,
	FOREIGN KEY customer_id REFERENCES DimCustomers(customer_id)
);

-- Creating fact marketing table
CREATE TABLE fact_marketing (
	fact_marketing_id int NOT NULL,
	month_of_deal int NOT NULL,
	avg_time_from_mql_to_seller decimal,
	best_media_type varchar,
	best_business_segment varchar
	FOREIGN KEY customer_id REFERENCES DimCustomers(customer_id)
);

-- Creating fact sales table
CREATE TABLE fact_sales (
	fact_sale_id int PRIMARY KEY,
	order_id int NOT NULL,
	customer_id int NOT NULL,
	product_id int NOT NULL,
	order_purchase_month int NOT NULL,
	seller_id int NOT NULL,
	total_payment_value decimal,
	total_freight_value decimal,
	avg_num_items int,
	avg_num_installments int,
	FOREIGN KEY customer_id REFERENCES DimCustomers(customer_id),
	FOREIGN KEY product_id REFERENCES DimProducts(product_id),
	FOREIGN KEY order_purchase_month REFERENCES DimDate2(date_id),
	FOREIGN KEY seller_id REFERENCES DimSellers(seller_id)
);

-- Creating fact delivery table
CREATE TABLE fact_delivery (
	fact_delivery_id int PRIMARY KEY,
	order_id int NOT NULL,
	customer_id int NOT NULL,
	geolocation_id int NOT NULL,
	order_purchase_date int,
	time_from_purchase_to_delivaryfact decimal,
	time_from_partner_to_customer decimal,
	time_from_purchase_to_payment decimal,
	time_from_purchase_to_carrier decimal,
	time_between_estimate_and_deliveryfact decimal,	
	FOREIGN KEY customer_id REFERENCES DimCustomers(customer_id),
	FOREIGN KEY geolocation_id REFERENCES DimGeolocation(geolocation_id),
	FOREIGN KEY order_purchase_date REFERENCES DimDate2(date_id)
);


-- Creating procedure to load and update dimensional tables

CREATE OR REPLACE PROCEDURE add_customers()
LANGUAGE plpgsql
AS $$
BEGIN
	-- Truncate destination table
-- 	TRUNCATE TABLE DimCustomers CASCADE;
-- 	TRUNCATE TABLE DimSellers CASCADE;	
-- 	TRUNCATE TABLE DimGeolocation CASCADE;	
-- 	TRUNCATE TABLE DimProducts CASCADE;	
-- 	TRUNCATE TABLE DimDate2 CASCADE;
-- 	TRUNCATE TABLE DimDate CASCADE;	

	-- Merging for dimcustomers table
	MERGE INTO DimCustomers DC
	USING olist_customers OC
	ON DC.customer_id = OC.customer_id
	WHEN NOT MATCHED THEN
		INSERT (customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state)
		VALUES (OC.customer_id, OC.customer_unique_id, OC.customer_zip_code_prefix, OC.customer_city, OC.customer_state)
	WHEN MATCHED THEN
  		UPDATE SET 
			customer_id = OC.customer_id,
			customer_unique_id = OC.customer_unique_id,
			customer_zip_code_prefix = OC.customer_zip_code_prefix,
			customer_city = OC.customer_city,
			customer_state = OC.customer_state;
	
	-- Merging for dimsellers table
	MERGE INTO DimSellers DS
	USING olist_sellers OS
	ON DS.seller_id = OS.seller_id
	WHEN NOT MATCHED THEN
		INSERT (seller_id, seller_zip_code_prefix, seller_city, seller_state)
		VALUES (OS.seller_id, OS.seller_zip_code_prefix, OS.seller_city, OS.seller_state)
	WHEN MATCHED THEN
  		UPDATE SET 
			seller_id = OS.seller_id,
			seller_zip_code_prefix = OS.seller_zip_code_prefix,
			seller_city = OS.seller_city,
			seller_state = OS.seller_state;	
			
	-- Merging for dimproducts table	
	MERGE INTO DimProducts DP
	USING 
	-- Creating CTE in order to add product category name in english to products table
		(WITH CTE_products AS (
			SELECT product_id, pct.product_category_name_english, product_photos_qty, product_weight_g, product_length_cm, product_height_cm, product_width_cm
			FROM olist_products op 
			JOIN product_category_translation pct ON op.product_category_name = pct.product_category_name)
		SELECT * FROM CTE_products) OP
	ON DP.product_id = OP.product_id
	WHEN NOT MATCHED THEN
		INSERT (product_id, product_category_name_english, product_photos_qty, product_weight_g, product_length_cm, product_height_cm, product_width_cm, start_date, end_date, iscurrent)
		VALUES (OP.product_id, OP.product_category_name_english, OP.product_photos_qty, OP.product_weight_g, OP.product_length_cm, OP.product_height_cm, OP.product_width_cm, NOW(), '9999-12-31', 'Y')
	WHEN MATCHED THEN
  		UPDATE SET 
			end_date = NOW(),
			iscurrent = 'N'
		INSERT (product_id, product_category_name_english, product_photos_qty, product_weight_g, product_length_cm, product_height_cm, product_width_cm, start_date, end_date, iscurrent)
		VALUES (OP.product_id, OP.product_category_name_english, OP.product_photos_qty, OP.product_weight_g, OP.product_length_cm, OP.product_height_cm, OP.product_width_cm, NOW(), '9999-12-31', 'Y')
END;
$$;

select * from dimproducts
truncate table dimproducts cascade


-- Checking table for SCD  type 1

UPDATE olist_sellers
SET seller_zip_code_prefix = 10000
WHERE seller_id = '3442f8959a84dea7ee197c632cb2df15';

select * from dimsellers
where seller_id = '3442f8959a84dea7ee197c632cb2df15'

select * from olist_sellers
where seller_id = '3442f8959a84dea7ee197c632cb2df15'

delete from olist_sellers
where seller_id = '3442f8959a84dea7ee197c632cb2df15'
-- 
-- Creating CTE to add english product category name into olist_products table instead of portugese
WITH CTE_products AS (
	SELECT product_id, pct.product_category_name_english, product_photos_qty, product_weight_g, product_length_cm, product_height_cm, product_width_cm
	FROM olist_products op 
	JOIN product_category_translation pct ON op.product_category_name = pct.product_category_name)
SELECT * FROM CTE_products;

select * from CTE_products;


WITH CTE_products AS (
	SELECT product_id, product_height_cm, product_width_cm
	FROM olist_products op 
	)
SELECt * FROM CTE_products

SELECt * FROM CTE_products






select * from DimProducts
where product_id = '732bd381ad09e530fe0a5f457d81becb'

select * from olist_products
where product_id = '732bd381ad09e530fe0a5f457d81becb'

update olist_products
set product_weight_g = 888
where product_id = '732bd381ad09e530fe0a5f457d81becb'



MERGE INTO DimProducts DP
USING (
  WITH CTE_products AS (
	SELECT product_id, pct.product_category_name_english, product_photos_qty, product_weight_g, product_length_cm, product_height_cm, product_width_cm
	FROM olist_products op 
	JOIN product_category_translation pct ON op.product_category_name = pct.product_category_name
  )
  SELECT * FROM CTE_products
) OP
ON DP.product_id = OP.product_id
WHEN NOT MATCHED THEN
	INSERT (product_id, product_category_name_english, product_photos_qty, product_weight_g, product_length_cm, product_height_cm, product_width_cm, start_date, end_date, iscurrent)
	VALUES (OP.product_id, OP.product_category_name_english, OP.product_photos_qty, OP.product_weight_g, OP.product_length_cm, OP.product_height_cm, OP.product_width_cm, NOW(), '9999-12-31', 'Y')
WHEN MATCHED AND DP.iscurrent = 'Y' THEN
	INSERT (product_id, product_category_name_english, product_photos_qty, product_weight_g, product_length_cm, product_height_cm, product_width_cm, start_date, end_date, iscurrent)
	VALUES (OP.product_id, OP.product_category_name_english, OP.product_photos_qty, OP.product_weight_g, OP.product_length_cm, OP.product_height_cm, OP.product_width_cm, NOW(), '9999-12-31', 'Y')

CREATE OR REPLACE FUNCTION insert_updates()
RETURNS TRIGGER LANGUAGE plpgsql
AS $$
BEGIN
	
END;
$$

INSERT INTO Dimproducts (product_id, product_category_name_english, product_photos_qty, product_weight_g, product_length_cm, product_height_cm, product_width_cm, start_date, end_date, iscurrent)
VALUES ('lallalla', 'lallalla', 0, 0, 0, 0, 0, NOW(), '9999-12-31', 'Y') RETURNING product_id AS xyz, product_category_name_english as englis, product_photos_qty, product_weight_g, product_length_cm, product_height_cm, product_width_cm, start_date, end_date, iscurrent, NOW() as timenow;

