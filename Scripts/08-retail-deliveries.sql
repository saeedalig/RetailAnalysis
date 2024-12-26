

USE DATABASE retaildb;

-- ####################################################################################
--  QUERY DIRECTLY FROM INTERNAL STAGE FOR deliveries
-- ####################################################################################

SELECT 
    t.$1 AS delivery_id,                  -- Delivery ID
    t.$2 AS order_id,                      -- Order ID
    t.$3 AS delivery_date,                 -- Delivery Date
    t.$4 AS delivery_status,               -- Delivery Status
    t.$5 AS delivery_partner_id,           -- Delivery Partner ID
    t.$6 AS tracking_number,               -- Tracking Number
    
    -- Metadata for audit
    metadata$filename as _stg_file_name, 
    CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', metadata$file_last_modified) as _stg_file_load_ts, -- Convert to IST
    metadata$file_content_key as _stg_file_md5,
    CURRENT_TIMESTAMP() as _copy_data_ts  -- Current timestamp
FROM 
    @retaildb.bronze_sch.csv_stg/data/deliveries.csv 
    (FILE_FORMAT => retaildb.bronze_sch.csv_ff) t;



-- ####################################################################################
-- CREATE raw_deliveries table INTO BRONZE LAYER TO STORE SOURCE DATA AS-IS
-- ####################################################################################

CREATE OR REPLACE TABLE retaildb.bronze_sch.raw_deliveries (
    delivery_id STRING,
    order_id STRING,
    delivery_date STRING,
    delivery_status STRING,
    delivery_partner_id STRING,
    tracking_number STRING,

    -- METADATA
    _stg_file_name STRING,                -- Staging file name
    _stg_file_load_ts TIMESTAMP,          -- File load timestamp
    _stg_file_md5 STRING,                 -- File MD5 hash
    _copy_data_ts TIMESTAMP               -- Data copy timestamp
);

-- ###### COPY INTO ######
COPY INTO retaildb.bronze_sch.raw_deliveries
FROM (
    SELECT 
        t.$1 AS delivery_id,
        t.$2 AS order_id,
        t.$3 AS delivery_date,  
        t.$4 AS delivery_status,
        t.$5 AS delivery_partner_id, 
        t.$6 AS tracking_number,

        -- Metadata for audit
        metadata$filename as _stg_file_name,
        CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', metadata$file_last_modified) as _stg_file_load_ts,  -- Convert to IST
        metadata$file_content_key as _stg_file_md5,
        CURRENT_TIMESTAMP() as _copy_data_ts                       -- Current timestamp
    FROM 
        @retaildb.bronze_sch.csv_stg/data/deliveries.csv t
)
FILE_FORMAT = (FORMAT_NAME = 'retaildb.bronze_sch.csv_ff')
ON_ERROR = ABORT_STATEMENT;

SELECT * FROM retaildb.bronze_sch.raw_deliveries;



-- ####################################################################################
-- CREATE clean_deliveries TABLE IN SILVER LAYER TO STORE CLEAN DATA
-- ####################################################################################

CREATE OR REPLACE TABLE retaildb.silver_sch.clean_deliveries (
    delivery_id NUMBER AUTOINCREMENT PRIMARY KEY,                    -- Business Key
    order_id NUMBER NOT NULL,                                         -- Foreign Key to orders dimension
    delivery_date DATE NOT NULL,                                      -- Cleaned and transformed delivery date
    delivery_status STRING NOT NULL,                                  -- Validated delivery status
    delivery_partner_id STRING NOT NULL,                              -- Delivery partner ID
    tracking_number STRING NOT NULL,                                  -- Tracking number
    
    -- Metadata from Bronze Layer
    last_updated_ts TIMESTAMP NOT NULL,                                -- Timestamp of the last update
    _stg_file_name STRING,                                             -- Staging file name
    _stg_file_load_ts TIMESTAMP,                                       -- File load timestamp
    _stg_file_md5 STRING,                                              -- File MD5 hash
    _copy_data_ts TIMESTAMP                                            -- Data copy timestamp
);

-- ###### MERGE INTO ######

-- SCD TYPE 1: Silver layer is not designed to maintain the historical record.
MERGE INTO retaildb.silver_sch.clean_deliveries AS target
USING (
    -- Deduplicate data from the raw table
    WITH ranked_deliveries AS (
        SELECT 
            delivery_id,
            order_id,
            CAST(delivery_date AS DATE) AS delivery_date,   -- Transform string to date
            delivery_status,
            delivery_partner_id,
            tracking_number,
            _stg_file_name,
            _stg_file_load_ts,
            _stg_file_md5,
            _copy_data_ts,
            ROW_NUMBER() OVER (PARTITION BY delivery_id ORDER BY _stg_file_load_ts DESC) AS row_num
        FROM retaildb.bronze_sch.raw_deliveries
    )
    SELECT 
        delivery_id,
        order_id,
        delivery_date,
        delivery_status,
        delivery_partner_id,
        tracking_number,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts
    FROM ranked_deliveries
    WHERE row_num = 1  -- Keep only the latest record for each delivery_id
) AS source

-- COMMON COLUMN TO MATCH
ON target.delivery_id = source.delivery_id
WHEN MATCHED THEN
    -- Update existing records with the latest data
    UPDATE SET
        target.order_id = source.order_id,
        target.delivery_date = source.delivery_date,
        target.delivery_status = source.delivery_status,
        target.delivery_partner_id = source.delivery_partner_id,
        target.tracking_number = source.tracking_number,
        
        target.last_updated_ts = CURRENT_TIMESTAMP(), -- Set last_updated_ts to the current timestamp
        target._stg_file_name = source._stg_file_name,
        target._stg_file_load_ts = source._stg_file_load_ts,
        target._stg_file_md5 = source._stg_file_md5,
        target._copy_data_ts = source._copy_data_ts
WHEN NOT MATCHED THEN
    -- Insert new records
    INSERT (
        delivery_id,
        order_id,
        delivery_date,
        delivery_status,
        delivery_partner_id,
        tracking_number,
        last_updated_ts,
        
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts
    ) VALUES (
        source.delivery_id,
        source.order_id,
        source.delivery_date,
        source.delivery_status,
        source.delivery_partner_id,
        source.tracking_number,
        CURRENT_TIMESTAMP(), -- Set last_updated_ts to current time
        source._stg_file_name,
        source._stg_file_load_ts,
        source._stg_file_md5,
        source._copy_data_ts
    );

SELECT * FROM retaildb.silver_sch.clean_deliveries;



-- ####################################################################################
-- CREATE fact_deliveries TABLE IN GOLD LAYER TO STORE FACT DATA
-- ####################################################################################

CREATE OR REPLACE TABLE retaildb.gold_sch.fact_deliveries (
    delivery_id NUMBER AUTOINCREMENT PRIMARY KEY,                    -- Business Key
    order_id NUMBER NOT NULL,                                          -- Foreign Key to orders dimension
    delivery_date DATE NOT NULL,                                       -- Cleaned and transformed delivery date
    delivery_status STRING NOT NULL,                                   -- Validated delivery status
    delivery_partner_id STRING NOT NULL,                               -- Delivery partner ID
    tracking_number STRING NOT NULL,                                   -- Tracking number
    last_updated_ts TIMESTAMP NOT NULL,                                 -- Timestamp of the last update
    CONSTRAINT fk_order FOREIGN KEY (order_id) REFERENCES retaildb.gold_sch.fact_orders (order_id)
);

-- ####################################################################################
-- MERGE INTO STATEMENT TO INSERT DATA FROM SILVER LAYER INTO GOLD LAYER
-- ####################################################################################

MERGE INTO retaildb.gold_sch.fact_deliveries AS target
USING retaildb.silver_sch.clean_deliveries AS source
ON target.delivery_id = source.delivery_id  -- Match on delivery_id
WHEN NOT MATCHED THEN
    INSERT (
        delivery_id,
        order_id,
        delivery_date,
        delivery_status,
        delivery_partner_id,
        tracking_number,
        last_updated_ts
    )
    VALUES (
        source.delivery_id,
        source.order_id,
        source.delivery_date,
        source.delivery_status,
        source.delivery_partner_id,
        source.tracking_number,
        CURRENT_TIMESTAMP()  -- Set the last_updated_ts to the current timestamp
    );

SELECT * FROM retaildb.gold_sch.fact_deliveries;



    