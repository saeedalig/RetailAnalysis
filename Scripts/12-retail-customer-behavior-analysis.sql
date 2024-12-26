USE DATABASE retaildb;
USE SCHEMA retaildb.gold_sch;


-- Total number of unique customers who made a purchase
SELECT 
    COUNT(DISTINCT customer_id) AS total_unique_customers
FROM retaildb.gold_sch.master_orders
WHERE order_status = 'Delivered';


-- Average number of orders per customer
SELECT 
    AVG(order_count) AS avg_order_frequency_per_customer
FROM (
    SELECT 
        customer_id, 
        COUNT(order_id) AS order_count
    FROM retaildb.gold_sch.master_orders
    GROUP BY customer_id
) AS customer_order_counts;


-- Top 10 customers by total spending
SELECT 
    customer_id,
    customer_name,
    SUM(total_value) AS total_spending
FROM retaildb.gold_sch.master_orders
GROUP BY customer_id, customer_name
ORDER BY total_spending DESC
LIMIT 10;


-- Average order value (AOV) per customer
SELECT 
    customer_id, 
    AVG(total_value) AS avg_order_value
FROM retaildb.gold_sch.master_orders
GROUP BY customer_id;


-- Customer lifetime value: Total spending by each customer
SELECT 
    customer_id, 
    SUM(total_value) AS customer_lifetime_value
FROM retaildb.gold_sch.master_orders
GROUP BY customer_id;

-- Most frequent purchase time by hour of the day
SELECT 
    DAY(order_date) AS daily_order,
    COUNT(order_id) AS orders_count
FROM retaildb.gold_sch.master_orders
GROUP BY daily_order
ORDER BY orders_count DESC
LIMIT 1;

-- Distribution of customer purchases by city
SELECT 
    customer_city, 
    COUNT(DISTINCT customer_id) AS unique_customers,
    SUM(total_value) AS total_sales
FROM retaildb.gold_sch.master_orders
GROUP BY customer_city
ORDER BY total_sales DESC;


-- Sales distribution by customer gender
SELECT 
    gender, 
    COUNT(DISTINCT customer_id) AS unique_customers,
    SUM(total_value) AS total_sales
FROM retaildb.gold_sch.master_orders 
GROUP BY gender
ORDER BY total_sales DESC;


-- Customer retention rate: Percentage of customers who made more than one purchase
SELECT 
    (SUM(CASE WHEN order_count > 1 THEN 1 ELSE 0 END) / 
     COUNT(DISTINCT customer_id)) * 100 AS customer_retention_rate
FROM (
    SELECT 
        customer_id, 
        COUNT(order_id) AS order_count
    FROM retaildb.gold_sch.master_orders
    WHERE order_status = 'Delivered'
    GROUP BY customer_id
) AS customer_order_counts;



















