
USE DATABASE retaildb;

-- ####################################################################################
--  QUERY DIRECTLY FROM INTERNAL STAGE FOR orders
-- ####################################################################################

SELECT 
    t.$1 AS order_id,                  -- Order ID
    t.$2 AS customer_id,               -- Customer ID
    t.$3 AS order_date,  -- Convert order_date to DATE format
    t.$4 AS order_status,              -- Order Status
    t.$5 AS total_value, -- Convert total_value to DECIMAL
    t.$6 AS address_id,                -- Address ID
    
    -- Metadata for audit
    metadata$filename as _stg_file_name, 
    CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', metadata$file_last_modified) as _stg_file_load_ts, -- Convert to IST
    metadata$file_content_key as _stg_file_md5,
    CURRENT_TIMESTAMP() as _copy_data_ts  -- Current timestamp
FROM 
    @retaildb.bronze_sch.csv_stg/data/orders.csv 
    (FILE_FORMAT => retaildb.bronze_sch.csv_ff) t;


-- ####################################################################################
-- CREATE raw_orders table INTO BRONZE LAYER TO STORE SOURCE DATA AS-IS
-- ####################################################################################

-- order_id,customer_id,order_date,order_status,total_value,address_id
CREATE OR REPLACE TABLE retaildb.bronze_sch.raw_orders (
    order_id STRING,
    customer_id STRING,
    order_date STRING,
    order_status STRING,
    total_value STRING,
    address_id STRING,

    -- METADATA
    _stg_file_name STRING,                -- Staging file name
    _stg_file_load_ts TIMESTAMP,          -- File load timestamp
    _stg_file_md5 STRING,                 -- File MD5 hash
    _copy_data_ts TIMESTAMP               -- Data copy timestamp
);

-- ###### COPY INTO ######
COPY INTO retaildb.bronze_sch.raw_orders
FROM (
    SELECT 
        t.$1 AS order_id,
        t.$2 AS customer_id,
        t.$3 AS order_date,  
        t.$4 AS order_status,
        t.$5 AS total_value, 
        t.$6 AS address_id,

        -- Metadata for audit
        metadata$filename as _stg_file_name,
        CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', metadata$file_last_modified) as _stg_file_load_ts,  -- Convert to IST
        metadata$file_content_key as _stg_file_md5,
        CURRENT_TIMESTAMP() as _copy_data_ts                       -- Current timestamp
    FROM 
        @retaildb.bronze_sch.csv_stg/data/orders.csv t
)
FILE_FORMAT = (FORMAT_NAME = 'retaildb.bronze_sch.csv_ff')
ON_ERROR = ABORT_STATEMENT;

SELECT * FROM retaildb.bronze_sch.raw_orders;




-- ####################################################################################
-- CREATE clean_orders TABLE IN SILVER LAYER TO STORE CLEAN DATA
-- ####################################################################################

CREATE OR REPLACE TABLE retaildb.silver_sch.clean_orders (
    order_id NUMBER AUTOINCREMENT PRIMARY KEY,                     -- Business Key
    customer_id NUMBER NOT NULL,                    -- Foreign Key to customer dimension
    order_date DATE NOT NULL,                       -- Cleaned and transformed order date
    order_status STRING NOT NULL,                   -- Validated order status
    total_value DECIMAL(10, 2) NOT NULL,            -- Corrected total value of the order
    address_id NUMBER NOT NULL,                     -- Foreign Key to address dimension
    
    -- Metadata from Bronze Layer
    last_updated_ts TIMESTAMP NOT NULL,             -- Timestamp of the last update
    _stg_file_name STRING,                          -- Staging file name
    _stg_file_load_ts TIMESTAMP,                    -- File load timestamp
    _stg_file_md5 STRING,                           -- File MD5 hash
    _copy_data_ts TIMESTAMP                         -- Data copy timestamp
);

-- ###### MERGE INTO ######

-- SCD TYPE 1: Silver layer is not designed to maintain the historical record.
MERGE INTO retaildb.silver_sch.clean_orders AS target
USING (
    -- Deduplicate data from the raw table
    WITH ranked_orders AS (
        SELECT 
            order_id,
            customer_id,
            CAST(order_date AS DATE) AS order_date,   -- Transform string to date
            order_status,
            CAST(total_value AS DECIMAL(10, 2)) AS total_value, -- Transform string to decimal
            address_id,
            _stg_file_name,
            _stg_file_load_ts,
            _stg_file_md5,
            _copy_data_ts,
            ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _stg_file_load_ts DESC) AS row_num
        FROM retaildb.bronze_sch.raw_orders
    )
    SELECT 
        order_id,
        customer_id,
        order_date,
        order_status,
        total_value,
        address_id,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts
    FROM ranked_orders
    WHERE row_num = 1  -- Keep only the latest record for each order_id
) AS source

-- COMMON COLUMN TO MATCH
ON target.order_id = source.order_id
WHEN MATCHED THEN
    -- Update existing records with the latest data
    UPDATE SET
        target.customer_id = source.customer_id,
        target.order_date = source.order_date,
        target.order_status = source.order_status,
        target.total_value = source.total_value,
        target.address_id = source.address_id,
        
        target.last_updated_ts = CURRENT_TIMESTAMP(), -- Set last_updated_ts to the current timestamp
        target._stg_file_name = source._stg_file_name,
        target._stg_file_load_ts = source._stg_file_load_ts,
        target._stg_file_md5 = source._stg_file_md5,
        target._copy_data_ts = source._copy_data_ts
WHEN NOT MATCHED THEN
    -- Insert new records
    INSERT (
        order_id,
        customer_id,
        order_date,
        order_status,
        total_value,
        address_id,
        last_updated_ts,
        
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts
    ) VALUES (
        source.order_id,
        source.customer_id,
        source.order_date,
        source.order_status,
        source.total_value,
        source.address_id,
        CURRENT_TIMESTAMP(), -- Set last_updated_ts to current time
        source._stg_file_name,
        source._stg_file_load_ts,
        source._stg_file_md5,
        source._copy_data_ts
    );

SELECT * FROM retaildb.silver_sch.clean_orders;





-- ####################################################################################
-- CREATE fact_orders TABLE IN GOLD LAYER TO STORE FINAL DATA
-- ####################################################################################

CREATE OR REPLACE TABLE retaildb.gold_sch.fact_orders (
    order_id NUMBER AUTOINCREMENT PRIMARY KEY,                     -- Business Key
    customer_id NUMBER NOT NULL,                    -- Foreign Key to customer dimension
    order_date DATE NOT NULL,                       -- Cleaned and transformed order date
    order_status STRING NOT NULL,                   -- Validated order status
    total_value DECIMAL(10, 2) NOT NULL,            -- Corrected total value of the order
    address_id NUMBER NOT NULL,                     -- Foreign Key to address dimension
    last_updated_ts TIMESTAMP NOT NULL,             -- Timestamp of the last update
    CONSTRAINT fk_customer FOREIGN KEY (customer_id) REFERENCES retaildb.gold_sch.dim_customers (customer_id)
);

-- ####################################################################################
-- MERGE INTO STATEMENT TO INSERT DATA FROM SILVER LAYER INTO GOLD LAYER
-- ####################################################################################

MERGE INTO retaildb.gold_sch.fact_orders AS target
USING retaildb.silver_sch.clean_orders AS source
ON target.order_id = source.order_id  -- Match on order_id
WHEN NOT MATCHED THEN
    INSERT (
        order_id,
        customer_id,
        order_date,
        order_status,
        total_value,
        address_id,
        last_updated_ts
    )
    VALUES (
        source.order_id,
        source.customer_id,
        source.order_date,
        source.order_status,
        source.total_value,
        source.address_id,
        CURRENT_TIMESTAMP()  -- Set the last_updated_ts to the current timestamp
    );


-- ####################################################################################
-- INSERT INTO STATEMENT TO INSERT DATA FROM SILVER LAYER INTO GOLD LAYER
-- ####################################################################################

INSERT INTO retaildb.gold_sch.fact_orders (
    order_id,
    customer_id,
    order_date,
    order_status,
    total_value,
    address_id,
    last_updated_ts
)
SELECT 
    order_id,
    customer_id,
    order_date,
    order_status,
    total_value,
    address_id,
    CURRENT_TIMESTAMP AS last_updated_ts -- Add the timestamp of the insert operation
FROM retaildb.silver_sch.clean_orders;

SELECT * FROM   retaildb.gold_sch.fact_orders;


