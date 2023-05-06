
-- Create staging tables 
CREATE TABLE olist_marketing_qualified_leads (
   mql_id varchar(50) PRIMARY KEY,
   first_contact_date timestamp,
	landing_page_id varchar(50),
	origin varchar(50));

CREATE TABLE olist_closed_deals (
   mql_id varchar(50) PRIMARY KEY,
   seller_id varchar(50),
	sdr_id varchar(50),
	sr_id varchar(50),
	won_date timestamp,
	business_segment varchar, 
	lead_type varchar,
	lead_behaviour_profile varchar,
	has_company varchar(50),
	has_gtin varchar(50),
	average_stock varchar(50),
	business_type varchar,
	declared_product_catalog_size decimal,
	declared_monthly_revenue decimal);

CREATE TABLE olist_customers (
   customer_id varchar(50) PRIMARY KEY,
   customer_unique_id varchar(50),
   customer_zip_code_prefix integer,
   customer_city varchar(50),
   customer_state varchar(50));

CREATE TABLE olist_geolocation (
    geolocation_zip_code_prefix INTEGER,
    geolocation_lat NUMERIC(9,6),
    geolocation_lng NUMERIC(9,6),
    geolocation_city TEXT,
    geolocation_state TEXT
);

CREATE TABLE olist_order_items (
    order_id VARCHAR(32),
    order_item_id INTEGER,
    product_id VARCHAR(32),
    seller_id VARCHAR(32),
    shipping_limit_date TIMESTAMP,
    price NUMERIC(15,2),
    freight_value NUMERIC(15,2)
);

CREATE TABLE olist_order_payments (
    order_id VARCHAR(32),
    payment_sequential INTEGER,
    payment_type VARCHAR(50),
    payment_installments INTEGER,
    payment_value NUMERIC(15,2)
);


CREATE TABLE olist_order_reviews (
    review_id VARCHAR(32),
    order_id VARCHAR(32),
    review_score INTEGER,
    review_comment_title VARCHAR(100),
    review_comment_message TEXT,
    review_creation_date TIMESTAMP,
    review_answer_timestamp TIMESTAMP
);

CREATE TABLE olist_orders (
    order_id VARCHAR(50),
    customer_id VARCHAR(50),
    order_status VARCHAR(20),
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP
);

CREATE TABLE olist_products (
    product_id VARCHAR(50) PRIMARY KEY,
    product_category_name VARCHAR(100),
    product_name_length INTEGER,
    product_description_length INTEGER,
    product_photos_qty INTEGER,
    product_weight_g INTEGER,
    product_length_cm INTEGER,
    product_height_cm INTEGER,
    product_width_cm INTEGER
);

CREATE TABLE olist_sellers (
    seller_id varchar(255) PRIMARY KEY,
    seller_zip_code_prefix integer,
    seller_city varchar(255),
    seller_state varchar(255)
);

CREATE TABLE product_category_translation (
    product_category_name varchar(255),
    product_category_name_english varchar(255)
);

-- Loading tables from csv to postgres server (in Terminal)
\copy olist_customers 
FROM '/Users/serikzhan/Desktop/Spring 2023/CS 779 Advanced Database Management/Project/Brazilian E-commerce/olist_customers_dataset.csv' 
DELIMITER ',' CSV HEADER;

-- Creating Dimensional tables for Data Warehouse
-- Creating Date Dimensional table with monthly grain
drop table dimdate2, dimdate
CREATE TABLE DimDate2 (
    date_id SERIAL PRIMARY KEY,
    year INT,
    quarter INT,
    month INT
);

-- Creating Date dimensional table with daily grain
CREATE TABLE DimDate (
    date_id SERIAL PRIMARY KEY,
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
	customer_id varchar(50),
	avg_review_score int,
	avg_time_to_fill_review interval,
	CONSTRAINT fk_review FOREIGN KEY (customer_id) REFERENCES DimCustomers(customer_id)
);
-- drop table fact_marketing
-- Creating fact marketing table
CREATE TABLE fact_marketing (
	month_of_deal int,
	avg_time_from_mql_to_seller interval,
	max_time interval,
	min_time interval,
	CONSTRAINT fk_dimdate2 FOREIGN KEY (month_of_deal) REFERENCES DimDate2(date_id)
);

-- Creating fact sales table
drop table fact_sales
select * from dimproducts
CREATE TABLE fact_sales (
	fact_sales_id SERIAL PRIMARY KEY,
	seller_id varchar(50) NOT NULL,
	dim_product_id int,
	order_purchase_month int NOT NULL,
	total_payment_value decimal,
	total_freight_value decimal,
	avg_num_installments int,
	CONSTRAINT fk_seller FOREIGN KEY (seller_id) REFERENCES DimSellers(seller_id),
	CONSTRAINT fk_product2 FOREIGN KEY (dim_product_id) REFERENCES DimProducts(dim_product_id),
	CONSTRAINT fk_purchase_month FOREIGN KEY (order_purchase_month) REFERENCES DimDate2(date_id)
);

-- Creating fact delivery table
drop table fact_delivery
CREATE TABLE fact_delivery (
	fact_delivery_id SERIAL PRIMARY KEY,
	order_id varchar(50) NOT NULL,
	customer_id varchar(50) NOT NULL,
	geolocation_id int NOT NULL,
	order_purchase_date int,
	time_from_purchase_to_delivaryfact interval,
	time_from_purchase_to_carrier interval,
	time_between_estimate_and_deliveryfact interval,	
	CONSTRAINT fk_customer3 FOREIGN KEY (customer_id) REFERENCES DimCustomers(customer_id),
	CONSTRAINT fk_geolocation3 FOREIGN KEY (geolocation_id) REFERENCES DimGeolocation(geolocation_id),
	CONSTRAINT fk_date3 FOREIGN KEY (order_purchase_date) REFERENCES DimDate(date_id)
);

-- Creating fact category ranks table
CREATE TABLE fact_category_ranks(
		product_category_name varchar,
		total_sales int,
		category_rank int);
		
		
-- Creating procedure to maintain scd type 1 dimensional tables
CREATE OR REPLACE PROCEDURE load_dimensions()
LANGUAGE plpgsql
AS $$
BEGIN
	-- Truncate destination table
	TRUNCATE TABLE DimCustomers CASCADE;
	TRUNCATE TABLE DimSellers CASCADE;	
	TRUNCATE TABLE DimGeolocation CASCADE;	
	TRUNCATE TABLE DimProducts CASCADE;	

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
			
	-- Merging for dimgeolocation table
	MERGE INTO DimGeolocation DG
	USING olist_geolocation OG
	ON DG.geolocation_zip_code_prefix = OG.geolocation_zip_code_prefix
	WHEN NOT MATCHED THEN
		INSERT (geolocation_zip_code_prefix, geolocation_lat, geolocation_lng, geolocation_city, geolocation_state)
		VALUES (OG.geolocation_zip_code_prefix, OG.geolocation_lat, OG.geolocation_lng, OG.geolocation_city, OG.geolocation_state)
	WHEN MATCHED THEN
  		UPDATE SET 
			geolocation_zip_code_prefix = OG.geolocation_zip_code_prefix,
			geolocation_lat = OG.geolocation_lat,
			geolocation_lng = OG.geolocation_lng,
			geolocation_city = OG.geolocation_city,
			geolocation_state = OG.geolocation_state;						
END; 
$$

CALL load_dimensions()
-- Creating procedure to maintain scd type 1 date dimensional tables
CREATE OR REPLACE PROCEDURE load_datedimensions()
LANGUAGE plpgsql
AS $$
BEGIN
	-- Insert records of purchase date from olist_orders table
	INSERT INTO DimDate (year, quarter, month, day)
    SELECT DISTINCT ON (year, quarter, month, day)
		DATE_PART('year', order_purchase_timestamp) AS year,
		DATE_PART('quarter', order_purchase_timestamp) AS quarter,
		DATE_PART('month', order_purchase_timestamp) AS month,
		DATE_PART('day', order_purchase_timestamp) AS day
    FROM olist_orders
	WHERE NOT EXISTS (SELECT 1 FROM dimdate 
					  WHERE year = date_part('year', olist_orders.order_purchase_timestamp) 
       				AND quarter = date_part('quarter', olist_orders.order_purchase_timestamp) 
        			AND month = date_part('month', olist_orders.order_purchase_timestamp)
					AND day = date_part('day', olist_orders.order_purchase_timestamp));

	-- Insert values (order_purchase_date and seller_won_date) to dimdate2 table 
	INSERT INTO DimDate2 (year, quarter, month)
    SELECT DISTINCT ON (year, quarter, month)
		DATE_PART('year', order_purchase_timestamp) AS year,
		DATE_PART('quarter', order_purchase_timestamp) AS quarter,
		DATE_PART('month', order_purchase_timestamp) AS month
    FROM olist_orders
	WHERE NOT EXISTS (SELECT 1 FROM dimdate2 
					  WHERE year = date_part('year', olist_orders.order_purchase_timestamp) 
       				AND quarter = date_part('quarter', olist_orders.order_purchase_timestamp) 
        			AND month = date_part('month', olist_orders.order_purchase_timestamp))
	UNION
	SELECT
	DATE_PART('year', won_date) AS year,
	DATE_PART('quarter', won_date) AS quarter,
	DATE_PART('month', won_date) AS month
	FROM olist_closed_deals
	WHERE NOT EXISTS (SELECT 1 FROM dimdate2 
					  WHERE year = date_part('year', olist_closed_deals.won_date) 
       				AND quarter = date_part('quarter', olist_closed_deals.won_date) 
        			AND month = date_part('month', olist_closed_deals.won_date));
END; 
$$

call load_datedimensions()
-- Creating CTE to add english product category name into olist_products table instead of portugese
WITH CTE_products AS (
	SELECT product_id, pct.product_category_name_english, product_photos_qty, product_weight_g, product_length_cm, product_height_cm, product_width_cm
	FROM olist_products op 
	JOIN product_category_translation pct ON op.product_category_name = pct.product_category_name)
SELECT * FROM CTE_products;

-- Create store procedure to maintain records in scd type 2 table (DimProducts)
CREATE OR REPLACE PROCEDURE maintain_scd2()
AS $$
BEGIN
-- Update values which exist in scd type 2 table
UPDATE dimproducts
SET end_date = NOW(), 
iscurrent = 'N'
WHERE end_date = '9999-12-31' AND
EXISTS (SELECT 1 FROM DimProducts
		JOIN olist_products ON DimProducts.product_id = olist_products.product_id
		WHERE DimProducts.product_id = olist_products.product_id);

-- Insert new records into target table
INSERT INTO DimProducts (product_id, product_category_name_english, product_photos_qty, product_weight_g, product_length_cm, product_height_cm, product_width_cm, start_date, end_date, iscurrent)
SELECT OP.product_id, OP.product_category_name_english, OP.product_photos_qty, OP.product_weight_g, OP.product_length_cm, OP.product_height_cm, OP.product_width_cm, NOW(), '9999-12-31', 'Y'
FROM (WITH CTE_products AS (
	SELECT product_id, pct.product_category_name_english, product_photos_qty, product_weight_g, product_length_cm, product_height_cm, product_width_cm
	FROM olist_products op 
	JOIN product_category_translation pct ON op.product_category_name = pct.product_category_name
)
SELECT * FROM CTE_products
) OP
WHERE NOT EXISTS (
	SELECT 1
	FROM DimProducts
	WHERE DimProducts.product_id = OP.product_id);
	
END; 
$$ LANGUAGE plpgsql;

CALL maintain_scd2()

DROP FUNCTION IF EXISTS load_facts();

-- Create store procedure to load fact tables
CREATE OR REPLACE PROCEDURE load_facts()
AS $$
BEGIN
	-- Empty tables before loading data
	TRUNCATE Fact_Marketing;
	TRUNCATE FactSales;
	TRUNCATE FactDelivery;
	TRUNCATE FactReview;
	
	-- Insert records to fact marketing table
	INSERT INTO Fact_Marketing(month_of_deal, avg_time_from_mql_to_seller, max_time, min_time)
	SELECT d.date_id, avg(ocd.won_date - omql.first_contact_date) as avg_time_from_mql_to_seller, 
	max(ocd.won_date - omql.first_contact_date) as max_time,
	min(ocd.won_date - omql.first_contact_date) as min_time
	FROM olist_closed_deals ocd
	JOIN olist_marketing_qualified_leads omql ON omql.mql_id = ocd.mql_id
	JOIN dimdate2 d ON d.year = EXTRACT(YEAR FROM ocd.won_date)
	AND d.month = EXTRACT(month FROM ocd.won_date)
	GROUP BY d.date_id;
	
	-- Insert records to fact review table
	INSERT INTO fact_review(customer_id, avg_review_score, avg_time_to_fill_review)
	SELECT dc.customer_id, avg(review_score), avg(oor.review_answer_timestamp - oor.review_creation_date)
	FROM olist_order_reviews oor
	JOIN olist_orders oo ON oo.order_id = oor.order_id
	JOIN Dimcustomers dc ON dc.customer_id = oo.customer_id
	group by dc.customer_id;
	
	-- Insert records to fact sales table
	INSERT INTO fact_sales(dim_product_id, order_purchase_month, seller_id, total_payment_value, total_freight_value, avg_num_installments)
	SELECT dp.dim_product_id, d.date_id, ds.seller_id,  sum(oop.payment_value), sum(ooi.freight_value), avg(oop.payment_installments)
	FROM olist_orders oo
	JOIN olist_order_items ooi ON ooi.order_id = oo.order_id
	JOIN dimdate2 d ON d.year = EXTRACT(YEAR FROM oo.order_purchase_timestamp)
	AND d.month = EXTRACT(month FROM oo.order_purchase_timestamp)
	JOIN olist_order_payments oop ON oop.order_id = oo.order_id
	JOIN DimProducts dp ON dp.product_id = ooi.product_id
	JOIN DimSellers ds ON ds.seller_id = ooi.seller_id
	group by d.date_id, ds.seller_id, dp.dim_product_id;

	-- Insert records to fact delivery table
	INSERT INTO fact_delivery(order_id, customer_id, geolocation_id, order_purchase_date, time_from_purchase_to_delivaryfact,
							 time_from_purchase_to_carrier, time_between_estimate_and_deliveryfact)
	SELECT oo.order_id, oo.customer_id, dg.geolocation_id, d1.date_id, avg(oo.order_delivered_customer_date - oo.order_purchase_timestamp),
	avg(oo.order_delivered_carrier_date - oo.order_purchase_timestamp), avg(oo.order_estimated_delivery_date - oo.order_delivered_customer_date)
	FROM olist_orders oo
	JOIN DimCustomers dc ON dc.customer_id = oo.customer_id
	JOIN DimGeolocation dg ON dg.geolocation_zip_code_prefix = dc.customer_zip_code_prefix
	JOIN dimdate d1 ON d1.year = EXTRACT(YEAR FROM oo.order_purchase_timestamp)
	AND d1.month = EXTRACT(month FROM oo.order_purchase_timestamp)
	AND d1.day = EXTRACT(day FROM oo.order_purchase_timestamp)
	GROUP BY oo.order_id, oo.customer_id, dg.geolocation_id, d1.date_id;
	
	-- Insert records to category ranks
	INSERT INTO fact_category_ranks(product_category_name, total_sales,	category_rank)
	SELECT DISTINCT dp.product_category_name_english, sum(ooi.price), RANK() OVER (ORDER BY sum(ooi.price) DESC) as category_rank
	FROM DimProducts dp
	JOIN olist_order_items ooi ON ooi.product_id = dp.product_id
	GROUP BY dp.product_category_name_english
	ORDER by category_rank;
END;
$$ LANGUAGE plpgsql
