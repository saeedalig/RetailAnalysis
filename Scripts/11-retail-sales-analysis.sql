USE DATABASE retaildb;
USE SCHEMA retaildb.gold_sch;



-- 1. Total Sales Revenue
SELECT 
    SUM(total_value) AS total_sales_revenue
FROM retaildb.gold_sch.master_orders;


-- 2. Average Order Value (AOV)

SELECT 
    ROUND(AVG(total_value),2) AS average_order_value
FROM retaildb.gold_sch.master_orders;


--3. Sales Growth (Month-over-Month)

SELECT 
    EXTRACT(MONTH FROM order_date) AS month,
    EXTRACT(YEAR FROM order_date) AS year,
    SUM(total_value) AS monthly_sales,
    LAG(SUM(total_value)) OVER (ORDER BY EXTRACT(YEAR FROM order_date), EXTRACT(MONTH FROM order_date)) AS previous_month_sales,
    (SUM(total_value) - 
        LAG(SUM(total_value)) OVER (ORDER BY EXTRACT(YEAR FROM order_date), EXTRACT(MONTH FROM order_date))) / 
        LAG(SUM(total_value)) OVER (ORDER BY EXTRACT(YEAR FROM order_date), EXTRACT(MONTH FROM order_date)) * 100 AS sales_growth_percentage
FROM retaildb.gold_sch.master_orders
WHERE order_status = 'Delivered'
GROUP BY EXTRACT(MONTH FROM order_date), EXTRACT(YEAR FROM order_date)
ORDER BY year, month;


-- 4. Units Sold

SELECT 
    SUM(quantity) AS total_units_sold
FROM retaildb.gold_sch.master_orders;


-- 5. Sales by Product Category

SELECT 
    category AS product_category,
    SUM(quantity * price_per_unit) AS total_sales_by_category
FROM retaildb.gold_sch.master_orders
GROUP BY category
ORDER BY total_sales_by_category DESC;

-- 6. Sales by Customer Location (City)

SELECT 
    customer_city,
    SUM(total_value) AS total_sales_by_city
FROM retaildb.gold_sch.master_orders
WHERE order_status = 'Delivered'
GROUP BY customer_city
ORDER BY total_sales_by_city DESC;

-- 7. Sales Conversion Rate (Assuming there's a visitors table for tracking customer visits)

SELECT 
    (COUNT(DISTINCT order_id) / COUNT(DISTINCT customer_id)) * 100 AS sales_conversion_rate
FROM retaildb.gold_sch.master_orders;

-- 8. Sales Return Rate

SELECT 
    (COUNT(return_id) / COUNT(order_id)) * 100 AS sales_return_rate
FROM retaildb.gold_sch.master_orders;

-- 9. Sales by Delivery Partner

SELECT 
    delivery_partner,
    SUM(total_value) AS total_sales_by_partner
FROM retaildb.gold_sch.master_orders
WHERE order_status = 'Delivered'
GROUP BY delivery_partner
ORDER BY total_sales_by_partner DESC;

-- 10. Top Selling Products

SELECT 
    product_name,
    SUM(quantity) AS total_units_sold,
    SUM(quantity * price_per_unit) AS total_sales
FROM retaildb.gold_sch.master_orders
WHERE order_status = 'Delivered'
GROUP BY product_name
ORDER BY total_sales DESC
LIMIT 10;




