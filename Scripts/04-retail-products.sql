USE DATABASE retaildb;

-- ####################################################################################
--  QUERY DIRECTLY FRON INTERNAL STAGE 
-- ####################################################################################

SELECT 
    t.$1 AS product_id,
    t.$2 AS product_name,
    t.$3 AS category,
    t.$4 AS price,
    t.$5 AS stock_quantity,
    
    -- Metadata for audit
    metadata$filename as _stg_file_name,
    CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', metadata$file_last_modified) as _stg_file_load_ts,  -- Convert to IST
    metadata$file_content_key as _stg_file_md5,
    CURRENT_TIMESTAMP() as _copy_data_ts                       -- Convert current timestamp to IST
FROM 
    @retaildb.bronze_sch.csv_stg/data/products.csv 
    (FILE_FORMAT => retaildb.bronze_sch.csv_ff) t;


-- ####################################################################################
-- CREATE raw_products table INTO BRONZE LAYER TO STORE SOURCE DATA AS-IS
-- ####################################################################################

-- product_id, product_name, category, price, stock_quantity
CREATE OR REPLACE TABLE retaildb.bronze_sch.raw_products (
    product_id STRING,
    product_name STRING,
    category STRING,
    price STRING,
    stock_quantity STRING,

    -- METADATA
    _stg_file_name STRING,                -- Staging file name
    _stg_file_load_ts TIMESTAMP,          -- File load timestamp
    _stg_file_md5 STRING,                 -- File MD5 hash
    _copy_data_ts TIMESTAMP               -- Data copy timestamp
);

-- ###### COPY INTO ######
COPY INTO retaildb.bronze_sch.raw_products
FROM (
    SELECT 
        t.$1 AS product_id,
        t.$2 AS product_name,
        t.$3 AS category,
        t.$4 AS price,
        t.$5 AS stock_quantity,
            
        -- Metadata for audit
        metadata$filename as _stg_file_name,
        CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', metadata$file_last_modified) as _stg_file_load_ts,  -- Convert to IST
        metadata$file_content_key as _stg_file_md5,
        CURRENT_TIMESTAMP() as _copy_data_ts                       -- Convert current timestamp to IST
    FROM @retaildb.bronze_sch.csv_stg/data/products.csv t
)
FILE_FORMAT = (FORMAT_NAME = 'retaildb.bronze_sch.csv_ff')
ON_ERROR = ABORT_STATEMENT;

SELECT * FROM retaildb.bronze_sch.raw_products;





-- ####################################################################################
-- CREATE clean_products table INTO SILVER LAYER TO STORE CLEAN DATA
-- ####################################################################################
DROP TABLE retaildb.silver_sch.clean_products;

CREATE OR REPLACE TABLE retaildb.silver_sch.clean_products (
    product_id NUMBER AUTOINCREMENT PRIMARY KEY,
    product_name STRING NOT NULL,
    category STRING NOT NULL,
    price NUMBER(10,2) NOT NULL,
    stock_quantity NUMBER NOT NULL,
    
    -- Metadata for SCD
    last_updated_ts TIMESTAMP NOT NULL,           -- Timestamp of the last update
    is_active STRING DEFAULT 'yes',               -- Record status: 'active' or 'inactive'
    
    -- Metadata from Bronze Layer
    _stg_file_name STRING,                        -- Staging file name
    _stg_file_load_ts TIMESTAMP,                  -- File load timestamp
    _stg_file_md5 STRING,                         -- File MD5 hash
    _copy_data_ts TIMESTAMP                       -- Data copy timestamp
);


-- ###### MERGE INTO ######

-- SCD TYPE 1: Silver layer is not designed to maintain the historical record.
MERGE INTO retaildb.silver_sch.clean_products AS target
USING (
    -- Deduplicate data from the raw table
    WITH ranked_products AS (
        SELECT 
            product_id,
            product_name,
            category,
            price,
            stock_quantity,
            _stg_file_name,
            _stg_file_load_ts,
            _stg_file_md5,
            _copy_data_ts,
            ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY _stg_file_load_ts DESC) AS row_num
        FROM retaildb.bronze_sch.raw_products
        WHERE product_id IS NOT NULL           -- handles null
    )
    SELECT 
        product_id,
        product_name,
        category,
        price,
        stock_quantity,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts
    FROM ranked_products
    WHERE row_num = 1             -- Keep only the latest record for each product_id
) AS source

-- COMMON COLUMN TO MATCH
ON target.product_id = source.product_id
WHEN MATCHED THEN
    -- Update existing records with the latest data
    UPDATE SET
        target.product_name = source.product_name,
        target.category = source.category,
        target.price = source.price,
        target.stock_quantity = source.stock_quantity,
        target.last_updated_ts = CURRENT_TIMESTAMP(), -- Set last_updated_ts to the current timestamp
        target._stg_file_name = source._stg_file_name,
        target._stg_file_load_ts = source._stg_file_load_ts,
        target._stg_file_md5 = source._stg_file_md5,
        target._copy_data_ts = source._copy_data_ts
WHEN NOT MATCHED THEN
    -- Insert new records
    INSERT (
        product_id,
        product_name,
        category,
        price,
        stock_quantity,
        last_updated_ts,
        is_active,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts
    ) VALUES (
        source.product_id,
        source.product_name,
        source.category,
        source.price,
        source.stock_quantity,
        CURRENT_TIMESTAMP,
        'yes',                  -- Default value for is_active
        source._stg_file_name,
        source._stg_file_load_ts,
        source._stg_file_md5,
        source._copy_data_ts
    );

SELECT * FROM retaildb.silver_sch.clean_products;




-- ####################################################################################
-- CREATE dim_products table INTO GOLD LAYER TO STORE CLEAN DATA WITH
-- ####################################################################################
DROP TABLE IF EXISTS retaildb.silver_sch.dim_products;

CREATE OR REPLACE TABLE retaildb.gold_sch.dim_products (
    product_id NUMBER AUTOINCREMENT PRIMARY KEY,
    product_name STRING NOT NULL,
    category STRING NOT NULL,
    price FLOAT NOT NULL,
    stock_quantity NUMBER NOT NULL,

    -- SCD Type 2 columns
    effective_start_date TIMESTAMP NOT NULL,            -- The date when the record became active
    effective_end_date TIMESTAMP,                       -- The date when the record was deactivated (if updated)
    is_current STRING DEFAULT 'yes',          -- Flag to indicate if the record is current ('yes' or 'no')
    
    last_updated_ts TIMESTAMP NOT NULL        -- Timestamp: helps to know when the last change occurred in the source system

);

-- ###### MERGE INTO ######
-- SCD TYPE 2: Maintain history of changes in the product records
MERGE INTO gold_sch.dim_products AS target
USING (
    -- Directly use the clean data from the Silver layer
    SELECT 
        product_id,
        product_name,
        category,
        price,
        stock_quantity,
        last_updated_ts,
    FROM silver_sch.clean_products
) AS source

-- Match on the business key (product_id) and only consider current records in the target table
ON target.product_id = source.product_id AND target.is_current = TRUE

WHEN MATCHED AND (
    -- Check if there are changes in the attributes
    target.product_name <> source.product_name OR
    target.category <> source.category OR
    target.price <> source.price OR
    target.stock_quantity <> source.stock_quantity
) THEN
    -- Close the current record by setting the end date and is_current flag
    UPDATE SET
        target.effective_end_date = CURRENT_TIMESTAMP,    -- Close the current record
        target.is_current = FALSE,                        -- Mark as not current
        target.last_updated_ts = source.last_updated_ts   -- Last updated timestamp

WHEN NOT MATCHED THEN
    -- Insert new record
    INSERT (
        product_id,
        product_name,
        category,
        price,
        stock_quantity,
        effective_start_date,
        effective_end_date,
        is_current,
        last_updated_ts
    ) VALUES (
        source.product_id,
        source.product_name,
        source.category,
        source.price,
        source.stock_quantity,
        CURRENT_TIMESTAMP,         -- Set effective_start_date to current timestamp
        NULL,                      -- Effective end date (NULL for active record)
        TRUE,                      -- Mark as current record
        source.last_updated_ts
    );

select * from retaildb.gold_sch.dim_products
