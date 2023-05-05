-- Loading tables from csv to postgres server (in Terminal)
\copy olist_customers 
FROM '/Users/serikzhan/Desktop/Spring 2023/CS 779 Advanced Database Management/Project/Brazilian E-commerce/olist_customers_dataset.csv' 
DELIMITER ',' CSV HEADER;

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


