USE DATABASE retaildb;

-- ####################################################################################
-- QUERY DIRECTLY FROM INTERNAL STAGE FOR returns
-- ####################################################################################

SELECT 
    t.$1 AS return_id,                    -- Return ID
    t.$2 AS order_id,                     -- Order ID
    t.$3 AS product_id,                   -- Product ID
    CAST(t.$4 AS DATE) AS return_date,    -- Return Date (converted to DATE)
    t.$5 AS return_reason,                -- Return Reason
    
    -- Metadata for audit
    metadata$filename AS _stg_file_name, 
    CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', metadata$file_last_modified) AS _stg_file_load_ts, -- Convert to IST
    metadata$file_content_key AS _stg_file_md5,
    CURRENT_TIMESTAMP() AS _copy_data_ts  -- Current timestamp
FROM 
    @retaildb.bronze_sch.csv_stg/data/returns.csv 
    (FILE_FORMAT => retaildb.bronze_sch.csv_ff) t;



-- ####################################################################################
-- CREATE raw_returns TABLE IN BRONZE LAYER TO STORE RAW DATA
-- ####################################################################################

CREATE OR REPLACE TABLE retaildb.bronze_sch.raw_returns (
    return_id STRING,
    order_id STRING,
    product_id STRING,
    return_date STRING,
    return_reason STRING,

    -- METADATA
    _stg_file_name STRING,                -- Staging file name
    _stg_file_load_ts TIMESTAMP,          -- File load timestamp
    _stg_file_md5 STRING,                 -- File MD5 hash
    _copy_data_ts TIMESTAMP               -- Data copy timestamp
);

-- ###### COPY INTO ######
COPY INTO retaildb.bronze_sch.raw_returns
FROM (
    SELECT 
        t.$1 AS return_id,                    -- Return ID
        t.$2 AS order_id,                     -- Order ID
        t.$3 AS product_id,                   -- Product ID
        t.$4 AS return_date,                  -- Return Date (as string initially)
        t.$5 AS return_reason,                -- Return Reason

        -- Metadata for audit
        metadata$filename AS _stg_file_name, 
        CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', metadata$file_last_modified) AS _stg_file_load_ts, -- Convert to IST
        metadata$file_content_key AS _stg_file_md5,
        CURRENT_TIMESTAMP() AS _copy_data_ts  -- Current timestamp
    FROM 
        @retaildb.bronze_sch.csv_stg/data/returns.csv t
)
FILE_FORMAT = (FORMAT_NAME = 'retaildb.bronze_sch.csv_ff')
ON_ERROR = ABORT_STATEMENT;

-- Verify the loaded data
SELECT * FROM retaildb.bronze_sch.raw_returns;



-- ####################################################################################
-- CREATE clean_returns TABLE IN SILVER LAYER TO STORE CLEANED DATA
-- ####################################################################################

CREATE OR REPLACE TABLE retaildb.silver_sch.clean_returns (
    return_id NUMBER,                        -- Return ID
    order_id NUMBER,                         -- Order ID
    product_id NUMBER,                       -- Product ID
    return_date DATE,                        -- Cleaned return date
    return_reason STRING,                    -- Cleaned return reason
    
    last_updated_ts TIMESTAMP NOT NULL,      -- Timestamp of the last update
    _stg_file_name STRING,                   -- Staging file name
    _stg_file_load_ts TIMESTAMP,             -- File load timestamp
    _stg_file_md5 STRING,                    -- File MD5 hash
    _copy_data_ts TIMESTAMP                  -- Data copy timestamp
);


-- ####################################################################################
-- MERGE INTO clean_returns TABLE IN SILVER LAYER
-- ####################################################################################

MERGE INTO retaildb.silver_sch.clean_returns AS target
USING (
    -- Deduplicate data from the raw_returns table
    WITH ranked_returns AS (
        SELECT 
            return_id,
            order_id,
            product_id,
            CAST(return_date AS DATE) AS return_date,    -- Convert return_date to DATE
            return_reason,
            _stg_file_name,
            _stg_file_load_ts,
            _stg_file_md5,
            _copy_data_ts,
            ROW_NUMBER() OVER (PARTITION BY return_id ORDER BY _stg_file_load_ts DESC) AS row_num
        FROM retaildb.bronze_sch.raw_returns
    )
    SELECT 
        return_id,
        order_id,
        product_id,
        return_date,
        return_reason,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts
    FROM ranked_returns
    WHERE row_num = 1  -- Keep only the latest record for each return_id
) AS source

-- COMMON COLUMN TO MATCH
ON target.return_id = source.return_id
WHEN MATCHED THEN
    -- Update existing records with the latest data
    UPDATE SET
        target.order_id = source.order_id,
        target.product_id = source.product_id,
        target.return_date = source.return_date,
        target.return_reason = source.return_reason,
        
        target.last_updated_ts = CURRENT_TIMESTAMP(), -- Set last_updated_ts to the current timestamp
        target._stg_file_name = source._stg_file_name,
        target._stg_file_load_ts = source._stg_file_load_ts,
        target._stg_file_md5 = source._stg_file_md5,
        target._copy_data_ts = source._copy_data_ts
WHEN NOT MATCHED THEN
    -- Insert new records
    INSERT (
        return_id,
        order_id,
        product_id,
        return_date,
        return_reason,
        last_updated_ts,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts
    ) VALUES (
        source.return_id,
        source.order_id,
        source.product_id,
        source.return_date,
        source.return_reason,
        CURRENT_TIMESTAMP(),  -- Set last_updated_ts to the current time
        source._stg_file_name,
        source._stg_file_load_ts,
        source._stg_file_md5,
        source._copy_data_ts
    );

-- Verify the loaded data
SELECT * FROM retaildb.silver_sch.clean_returns;



-- ####################################################################################
-- CREATE fact_returns TABLE IN GOLD LAYER TO STORE FINAL DATA
-- ####################################################################################

CREATE OR REPLACE TABLE retaildb.gold_sch.fact_returns (
    return_id NUMBER PRIMARY KEY,                 -- Return ID (Business Key)
    order_id NUMBER NOT NULL,                     -- Foreign Key to the orders dimension
    product_id NUMBER NOT NULL,                   -- Foreign Key to the products dimension
    return_date DATE NOT NULL,                    -- Cleaned return date
    return_reason STRING NOT NULL,                -- Return reason
    last_updated_ts TIMESTAMP NOT NULL,           -- Timestamp of the last update
    CONSTRAINT fk_order FOREIGN KEY (order_id) REFERENCES retaildb.gold_sch.fact_orders (order_id),  -- FK to orders
    CONSTRAINT fk_product FOREIGN KEY (product_id) REFERENCES retaildb.gold_sch.dim_products (product_id)  -- FK to products
);




-- ####################################################################################
-- INSERT INTO fact_returns TABLE IN GOLD LAYER
-- ####################################################################################

INSERT INTO retaildb.gold_sch.fact_returns (
    return_id,
    order_id,
    product_id,
    return_date,
    return_reason,
    last_updated_ts
)
SELECT 
    return_id,
    order_id,
    product_id,
    return_date,
    return_reason,
    CURRENT_TIMESTAMP() AS last_updated_ts  -- Set last_updated_ts to the current timestamp
FROM retaildb.silver_sch.clean_returns;

-- Verify the data in the gold layer
SELECT * FROM retaildb.gold_sch.fact_returns;















    