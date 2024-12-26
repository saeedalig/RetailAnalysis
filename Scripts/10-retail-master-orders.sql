USE DATABASE retaildb;

-- Creating master orders table which serves as source of truth for further aggeration so as to minimize join operations. 

CREATE OR REPLACE TABLE retaildb.gold_sch.master_orders AS
SELECT 
    o.order_id,                            -- Unique Order ID
    o.customer_id,                         -- Customer ID
    o.order_date,                          -- Order Date
    o.order_status,                        -- Order Status (e.g., completed, pending)
    o.total_value,                         -- Total Order Value
    o.address_id,                          -- Address ID (for location-based analysis)
    oi.product_id,                         -- Product ID (for product-level analysis)
    oi.quantity,                           -- Quantity of product ordered
    oi.price_per_unit,                     -- Price per unit of product
    p.product_name,                        -- Product Name
    p.category,                            -- Product Category
    p.price,                               -- Product Price (for revenue analysis)
    p.stock_quantity,                      -- Product Stock (for inventory analysis)
    c.name AS customer_name,               -- Customer Name
    c.gender,                              -- Customer Gender
    c.dob AS customer_dob,                 -- Customer Date of Birth
    c.phone_number,                        -- Customer Phone Number
    c.joining_date AS customer_joining_date, -- Customer Joining Date
    ca.city AS customer_city,              -- Customer City (for location-based analysis)
    ca.state AS customer_state,            -- Customer State
    ca.country AS customer_country,        -- Customer Country
    dp.partner_name AS delivery_partner,   -- Delivery Partner Name
    dp.contact_number AS delivery_partner_contact, -- Delivery Partner Contact
    d.delivery_date,                       -- Delivery Date
    d.delivery_status,                     -- Delivery Status (e.g., delivered, in transit)
    r.return_date,                         -- Return Date (if any)
    r.return_reason,                       -- Reason for Return
    p.price * oi.quantity AS product_revenue, -- Product revenue (price * quantity)
    CURRENT_TIMESTAMP() AS data_insert_ts   -- Timestamp for when the data is inserted

FROM retaildb.gold_sch.fact_orders o
JOIN retaildb.gold_sch.fact_order_items oi ON o.order_id = oi.order_id
JOIN retaildb.gold_sch.dim_products p ON oi.product_id = p.product_id
JOIN retaildb.gold_sch.dim_customers c ON o.customer_id = c.customer_id
JOIN retaildb.gold_sch.dim_customer_addresses ca ON c.customer_id = ca.customer_id
JOIN retaildb.gold_sch.fact_deliveries d ON o.order_id = d.order_id
LEFT JOIN retaildb.gold_sch.dim_delivery_partners dp ON d.delivery_partner_id = dp.delivery_partner_id
LEFT JOIN retaildb.gold_sch.fact_returns r ON o.order_id = r.order_id AND oi.product_id = r.product_id
WHERE o.order_status = 'Delivered';  -- Optional filter to include only completed orders



SELECT * FROM retaildb.gold_sch.master_orders;







