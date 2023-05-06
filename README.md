## Olist E-commerce Data Warehouse
This is my final project for CS 779 Advanced Database Management

This project is a data warehousing solution for Olist, a Brazilian e-commerce platform. The objective of this project is to build a robust data warehouse that stores all of Olist's data, and provides business insights and analytics to help Olist's management make informed decisions.

### Data sources
1. https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
2. https://www.kaggle.com/datasets/olistbr/marketing-funnel-olist

### Background

Olist is a Brazilian e-commerce platform that connects small businesses to marketplaces through a single platform. Since its inception, Olist has helped thousands of businesses get started and reach a wider audience. As the company grew, they realized the need to organize their data and make it more accessible to stakeholders.

### Project Description

This project involves building a data warehouse that stores all of Olist's data. The data is extracted from multiple sources, including CSV files, and loaded into a PostgreSQL database. The data is then transformed using SQL scripts and loaded into a star schema consisting of fact and dimension tables.

### The project consists of the following steps:

**Extract:** The data is extracted from various sources, including CSV files, and loaded into a PostgreSQL database.  
**Transform:** The data is transformed using SQL scripts to create a star schema consisting of fact and dimension tables. The dimension tables include Date, Customer, Seller, Product, and Geolocation, while the fact tables include FactSales, FactReview, FactDelivery, and FactMarketing.  
**Load:** The transformed data is loaded into the PostgreSQL database.  
**Maintain:** Stored procedures are created to maintain the data in the dimension tables using SCD Type 2.  
**Analyze:** Business insights and analytics are generated using SQL queries.  

### Tools and Technologies

The following tools and technologies were used in this project:

PostgreSQL: A powerful, open-source relational database management system.  
SQL: A standard language used for querying and manipulating relational databases.  
Tableau: Used for visualizations.

### Conclusion
The Olist E-commerce Data Warehouse provides a scalable and robust solution for storing and analyzing Olist's data. The data warehouse is designed to handle large amounts of data and provide business insights and analytics to help Olist's management make informed decisions. With this data warehouse, Olist can better understand its customers, products, and sales trends, and make data-driven decisions to grow its business.
