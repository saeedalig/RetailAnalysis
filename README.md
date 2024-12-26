# **Retail Analysis Project**

## **Contents**
- [Problem Statement](#problem-statement)
- [Solution Overview](#solution-overview)
- [Prerequisites](#prerequisites)
- [Environment Setup](#environment-setup)
- [Key Features](#key-features)
- [Medallion Architecture](#medallion-architecture)
- [Analysis and Reporting](#analysis-and-reporting)
- [Enhancement](#enhancement)

---


## **Problem Statement**  
[Back to Contents](#contents)  
Retail businesses need a robust data infrastructure to analyze sales trends and customer behavior effectively. The challenge is to design a scalable system that handles raw data, ensures data quality, and provides actionable insights.

## **Solution Overview**  
[Back to Contents](#contents)  
This project addresses the problem by implementing a retail data warehouse in Snowflake. The solution involves generating synthetic retail data, organizing it using the Medallion architecture, and enabling advanced analytics through data modeling and transformation.

## **Prerequisites**  
[Back to Contents](#contents)  
Before starting this project, ensure the following prerequisites are met:
- **Python**: Python 3.x installed on your local machine.
- **Snowflake Account**: A Snowflake account to create and manage the data warehouse.
- **SQL**: Solid understanding of SQL to query and manipulate the data in Snowflake.

## **Environment Setup**  
[Back to Contents](#contents)  
SQL Script: [Scripts/00-initial-setup.sql](Scripts/00-initial-setup.sql)

The environment setup includes:
- **Virtual Warehouse** (`retail_wh`): A compute resource for running queries.
- **Database** (`retaildb`): The container for storing all data objects.
- **Schemas** (`bronze_sch`, `silver_sch`, `gold_sch`): Logical data storage layers for raw, cleaned, and transformed data.
- **Internal Stage** (`csv_stg`): Temporary storage for loading CSV data.
- **File Format** (`csv_ff`): Defines the structure of CSV files for loading into Snowflake.


## **Key Features**  
[Back to Contents](#contents)  
- **Data Generation**: Created synthetic retail data using the Faker library for customers, products, transactions, and locations.  
- **Data Warehouse**: Built a Snowflake-based data warehouse following the Medallion architecture.  
- **Medallion Architecture**:
    - **Bronze Layer**: Stored raw data directly from the internal stage.  
    - **Silver Layer**: Performed deduplication, null handling, and implemented SCD Type 1.  
    - **Gold Layer**: Applied SCD Type 2 for historical tracking and modeled fact and dimension tables.  

- **Data Modeling**: Designed a star schema with dimension and fact tables. Also created a master orders table using multiple joins  
    - **Master Orders Table**: The master_orders table in the Gold layer is designed to consolidate all relevant transactional data into a single, optimized table. It serves as the central fact table for retail sales analysis, incorporating information from multiple source tables like orders, order items, products, customers, customer addresses, deliveries, delivery partners, and returns.

    **Benefits in Terms of Performance and Computation Cost:**
    - **Performance**: The denormalized structure of the master_orders table allows for faster queries by eliminating the need for complex joins. This is especially important in large-scale datasets, where the time saved by avoiding multiple joins can significantly improve query performance.
    - **Cost Efficiency**: In cloud-based data warehouses like Snowflake, query costs are often based on the computational resources used during query execution. By reducing the number of joins and simplifying the query structure, the master_orders table helps minimize the computation time and, consequently, the associated costs.
    - **Simplified Reporting**: Since the table includes all relevant data in one place, it makes it easier for analysts to generate reports and insights without needing to reference multiple tables. This results in a more efficient and streamlined reporting process.

- **Analysis**: Generated insights on sales trends and customer behavior through SQL queries.  


## **Medallion Architecture**  
[Back to Contents](#contents)  

The Medallion Architecture organizes data into three distinct layers: Bronze, Silver, and Gold, each serving a specific purpose in the data pipeline.

### **Bronze Layer**  

- **Purpose**: Acts as the raw data storage layer.  
- **Process**: Source data is loaded directly into tables from the internal stage `csv_stg`.  
- **Details**: Data is ingested as-is, without any transformations, ensuring that the original data remains intact for traceability.

### **Silver Layer**  

- **Purpose**: Cleans and transforms data for analytical readiness.  
- **Process**:  
  - **Data Cleaning**: Performed deduplication and handled null values to ensure data quality.  
  - **Transformation**: Standardized data types to maintain consistency.  
  - **SCD Type 1**: Applied to dimension tables to update records with the latest information.  
    - **Why SCD Type 1?**: The Silver layer focuses on providing the most up-to-date data without maintaining historical records, making it suitable for scenarios where only current state analysis is needed.  
- **Details**: The cleaned and transformed data is loaded into tables in the Silver layer.

### **Gold Layer**  

- **Purpose**: Serves as the final layer for analytical and reporting purposes.  
- **Process**:  
  - Data from the Silver layer is loaded into the Gold layer with primary and foreign key relationships to enable data modeling.  
  - **SCD Type 2**: Applied to dimension tables to maintain historical records.  
    - **Why SCD Type 2?**: This allows tracking of changes over time, enabling historical analysis and trend identification, which is critical for business insights.  
- **Details**: The Gold layer supports complex queries and reporting, providing a comprehensive view of the data.  


## **Analysis and Reporting**  
[Back to Contents](#contents)

### **Sales Performance Analysis**
This section provides insights into overall sales metrics, product performance, and trends across various dimensions. The analysis includes key performance indicators such as total sales revenue, average order value, sales growth, units sold, sales by product category, customer location, sales conversion rate, return rate, and the top-selling products.

- SQL Script: [Scripts/11-retail-sales-analysis.sql](Scripts/11-retail-sales-analysis.sql)

1. **Total Sales Revenue**: Measures the total revenue generated from sales.
2. **Average Order Value (AOV)**: Calculates the average value of each order placed.
3. **Sales Growth (Month-over-Month)**: Analyzes sales growth by comparing monthly sales figures.
4. **Units Sold**: Tracks the total number of units sold during the period.
5. **Sales by Product Category**: Breaks down total sales by product category.
6. **Sales by Customer Location (City)**: Shows sales distribution across different cities.
7. **Sales Conversion Rate**: Measures the percentage of visitors who made a purchase.
8. **Sales Return Rate**: Tracks the percentage of products returned by customers.
9. **Sales by Delivery Partner**: Analyzes sales performance by delivery partners.
10. **Top Selling Products**: Identifies the top-performing products based on sales volume.

### **Customer Insights Analysis**
This section focuses on customer behavior, retention, and demographics, helping to understand customer purchasing patterns. The analysis covers unique customer counts, order frequency, top spenders, customer lifetime value, and other behavioral insights.

- SQL Script: [Scripts/12-customer-behavior-analysis.sql](Scripts/12-customer-behavior-analysis.sql)

1. **Total Number of Unique Customers Who Made a Purchase**: Measures the number of distinct customers who made a purchase.
2. **Average Number of Orders Per Customer**: Shows the average number of orders placed by each customer.
3. **Top 10 Customers by Total Spending**: Identifies the top 10 customers based on total spending.
4. **Average Order Value (AOV) Per Customer**: Measures the average value of orders for each customer.
5. **Customer Lifetime Value**: Tracks the total spending by each customer over time.
6. **Most Frequent Purchase Time by Hour of the Day**: Identifies the peak hours when purchases are made.
7. **Distribution of Customer Purchases by City**: Shows the distribution of purchases by customer city.
8. **Sales Distribution by Customer Gender**: Breaks down sales by customer gender.
9. **Customer Retention Rate**: Measures the percentage of customers who made more than one purchase.



### **Enhancement**
I wiil be enhancing the project by automating  and integrating BI Tools.

- **Automation**: Leverage Snowflake's `tasks` and `streams` for automating ETL processes. You may also integrate `Apache Airflow` for orchestrating workflows to improve efficiency.
- **BI Tools**: Integrate BI tools like Power BI and Tableau to provide real-time reporting and interactive dashboards for better data visualization and decision-making.





