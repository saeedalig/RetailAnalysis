USE DATABASE retaildb;


-- ####################################################################################
--  QUERY DIRECTLY FROM INTERNAL STAGE FOR order_items
-- ####################################################################################

SELECT 
    t.$1 AS order_item_id,            -- Order Item ID
    t.$2 AS order_id,                 -- Order ID
    t.$3 AS product_id,               -- Product ID
    t.$4 AS quantity,                 -- Quantity
    t.$5 AS price_per_unit,           -- Price Per Unit
    
    -- Metadata for audit
    metadata$filename as _stg_file_name, 
    CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', metadata$file_last_modified) as _stg_file_load_ts, -- Convert to IST
    metadata$file_content_key as _stg_file_md5,
    CURRENT_TIMESTAMP() as _copy_data_ts  -- Current timestamp
FROM 
    @retaildb.bronze_sch.csv_stg/data/order_items.csv 
    (FILE_FORMAT => retaildb.bronze_sch.csv_ff) t;

-- ####################################################################################
-- CREATE raw_order_items table INTO BRONZE LAYER TO STORE SOURCE DATA AS-IS
-- ####################################################################################

-- order_item_id,order_id,product_id,quantity,price_per_unit
CREATE OR REPLACE TABLE retaildb.bronze_sch.raw_order_items (
    order_item_id STRING,
    order_id STRING,
    product_id STRING,
    quantity STRING,
    price_per_unit STRING,

    -- METADATA
    _stg_file_name STRING,                -- Staging file name
    _stg_file_load_ts TIMESTAMP,          -- File load timestamp
    _stg_file_md5 STRING,                 -- File MD5 hash
    _copy_data_ts TIMESTAMP               -- Data copy timestamp
);

-- ###### COPY INTO ######
COPY INTO retaildb.bronze_sch.raw_order_items
FROM (
    SELECT 
        t.$1 AS order_item_id,
        t.$2 AS order_id,
        t.$3 AS product_id,  
        t.$4 AS quantity,
        t.$5 AS price_per_unit,

        -- Metadata for audit
        metadata$filename as _stg_file_name,
        CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', metadata$file_last_modified) as _stg_file_load_ts,  -- Convert to IST
        metadata$file_content_key as _stg_file_md5,
        CURRENT_TIMESTAMP() as _copy_data_ts                       -- Current timestamp
    FROM 
        @retaildb.bronze_sch.csv_stg/data/order_items.csv t
)
FILE_FORMAT = (FORMAT_NAME = 'retaildb.bronze_sch.csv_ff')
ON_ERROR = ABORT_STATEMENT;

-- Query to check the data loaded
SELECT * FROM retaildb.bronze_sch.raw_order_items;


-- ####################################################################################
-- CREATE clean_order_items TABLE IN SILVER LAYER TO STORE CLEAN DATA
-- ####################################################################################

CREATE OR REPLACE TABLE retaildb.silver_sch.clean_order_items (
    order_item_id NUMBER AUTOINCREMENT PRIMARY KEY,    -- Business Key
    order_id NUMBER NOT NULL,                           -- Foreign Key to orders table
    product_id NUMBER NOT NULL,                         -- Foreign Key to product dimension
    quantity DECIMAL(10, 2) NOT NULL,                   -- Cleaned quantity of the product
    price_per_unit DECIMAL(10, 2) NOT NULL,             -- Cleaned price per unit
    total_value DECIMAL(10, 2) NOT NULL,                -- Cleaned total value for the order item

    -- Metadata from Bronze Layer
    last_updated_ts TIMESTAMP NOT NULL,                 -- Timestamp of the last update
    _stg_file_name STRING,                              -- Staging file name
    _stg_file_load_ts TIMESTAMP,                        -- File load timestamp
    _stg_file_md5 STRING,                               -- File MD5 hash
    _copy_data_ts TIMESTAMP                             -- Data copy timestamp
);

-- ###### MERGE INTO ######

-- SCD TYPE 1: Silver layer is not designed to maintain the historical record.
MERGE INTO retaildb.silver_sch.clean_order_items AS target
USING (
    -- Deduplicate data from the raw table
    WITH ranked_order_items AS (
        SELECT 
            order_item_id,
            order_id,
            product_id,
            CAST(quantity AS DECIMAL(10, 2)) AS quantity,  -- Transform string to decimal
            CAST(price_per_unit AS DECIMAL(10, 2)) AS price_per_unit, -- Transform string to decimal
            CAST(quantity AS DECIMAL(10, 2)) * CAST(price_per_unit AS DECIMAL(10, 2)) AS total_value, -- Calculate total value
            _stg_file_name,
            _stg_file_load_ts,
            _stg_file_md5,
            _copy_data_ts,
            ROW_NUMBER() OVER (PARTITION BY order_item_id ORDER BY _stg_file_load_ts DESC) AS row_num
        FROM retaildb.bronze_sch.raw_order_items
    )
    SELECT 
        order_item_id,
        order_id,
        product_id,
        quantity,
        price_per_unit,
        total_value,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts
    FROM ranked_order_items
    WHERE row_num = 1  -- Keep only the latest record for each order_item_id
) AS source

-- COMMON COLUMN TO MATCH
ON target.order_item_id = source.order_item_id
WHEN MATCHED THEN
    -- Update existing records with the latest data
    UPDATE SET
        target.order_id = source.order_id,
        target.product_id = source.product_id,
        target.quantity = source.quantity,
        target.price_per_unit = source.price_per_unit,
        target.total_value = source.total_value,
        
        target.last_updated_ts = CURRENT_TIMESTAMP(), -- Set last_updated_ts to the current timestamp
        target._stg_file_name = source._stg_file_name,
        target._stg_file_load_ts = source._stg_file_load_ts,
        target._stg_file_md5 = source._stg_file_md5,
        target._copy_data_ts = source._copy_data_ts
WHEN NOT MATCHED THEN
    -- Insert new records
    INSERT (
        order_item_id,
        order_id,
        product_id,
        quantity,
        price_per_unit,
        total_value,
        last_updated_ts,
        
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts
    ) VALUES (
        source.order_item_id,
        source.order_id,
        source.product_id,
        source.quantity,
        source.price_per_unit,
        source.total_value,
        CURRENT_TIMESTAMP(), -- Set last_updated_ts to current time
        source._stg_file_name,
        source._stg_file_load_ts,
        source._stg_file_md5,
        source._copy_data_ts
    );

-- Query to check the data loaded
SELECT * FROM retaildb.silver_sch.clean_order_items;



-- ####################################################################################
-- CREATE fact_order_items TABLE IN GOLD LAYER TO STORE FINAL DATA
-- ####################################################################################

CREATE OR REPLACE TABLE retaildb.gold_sch.fact_order_items (
    order_item_id NUMBER AUTOINCREMENT PRIMARY KEY,    -- Business Key
    order_id NUMBER NOT NULL,                           -- Foreign Key to orders table
    product_id NUMBER NOT NULL,                         -- Foreign Key to product dimension
    quantity DECIMAL(10, 2) NOT NULL,                   -- Cleaned quantity of the product
    price_per_unit DECIMAL(10, 2) NOT NULL,             -- Cleaned price per unit
    total_value DECIMAL(10, 2) NOT NULL,                -- Cleaned total value for the order item
    last_updated_ts TIMESTAMP NOT NULL,                 -- Timestamp of the last update
    CONSTRAINT fk_order FOREIGN KEY (order_id) REFERENCES retaildb.gold_sch.fact_orders (order_id),
    CONSTRAINT fk_product FOREIGN KEY (product_id) REFERENCES retaildb.gold_sch.dim_products (product_id)
);

-- ####################################################################################
-- MERGE INTO STATEMENT TO INSERT DATA FROM SILVER LAYER INTO GOLD LAYER
-- ####################################################################################

MERGE INTO retaildb.gold_sch.fact_order_items AS target
USING retaildb.silver_sch.clean_order_items AS source
ON target.order_item_id = source.order_item_id  -- Match on order_item_id
WHEN NOT MATCHED THEN
    INSERT (
        order_item_id,
        order_id,
        product_id,
        quantity,
        price_per_unit,
        total_value,
        last_updated_ts
    )
    VALUES (
        source.order_item_id,
        source.order_id,
        source.product_id,
        source.quantity,
        source.price_per_unit,
        source.total_value,
        CURRENT_TIMESTAMP()  -- Set the last_updated_ts to the current timestamp
    );


-- ####################################################################################
-- INSERT INTO STATEMENT TO INSERT DATA FROM SILVER LAYER INTO GOLD LAYER
-- ####################################################################################

INSERT INTO retaildb.gold_sch.fact_order_items (
    order_item_id,
    order_id,
    product_id,
    quantity,
    price_per_unit,
    total_value,
    last_updated_ts
)
SELECT 
    order_item_id,
    order_id,
    product_id,
    quantity,
    price_per_unit,
    total_value,
    CURRENT_TIMESTAMP AS last_updated_ts -- Add the timestamp of the insert operation
FROM retaildb.silver_sch.clean_order_items;

-- Query to check the data loaded into fact_order_items
SELECT * FROM retaildb.gold_sch.fact_order_items;












