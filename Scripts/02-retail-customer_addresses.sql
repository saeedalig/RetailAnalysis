USE DATABASE retaildb;
USE SCHEMA retaildb.bronze_sch ;

SELECT CURRENT_TIMESTAMP();

SELECT 
    t.$1 AS address_id,
    t.$2 AS customer_id,
    t.$3 AS address,
    t.$4 AS city,
    t.$5 AS state,
    t.$6 AS zip_code,
    t.$7 AS country,
    
    -- Metadata for audit
    metadata$filename as _stg_file_name,
    CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', metadata$file_last_modified) as _stg_file_load_ts,  -- Convert to IST
    metadata$file_content_key as _stg_file_md5,
    --CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', CURRENT_TIMESTAMP()) as _copy_data_ts
    CURRENT_TIMESTAMP() as _copy_data_ts                       -- Convert current timestamp to IST
FROM 
    @retaildb.bronze_sch.csv_stg/data/customer_addresses.csv 
    (FILE_FORMAT => retaildb.bronze_sch.csv_ff) t;

-- ####################################################################################
-- CREATE raw_customer_addresses table INTO BRONZE LAYER TO STORE SOURCE DATA AS-IS
-- ####################################################################################

-- address_id,customer_id,address,city,state,zip_code,country
CREATE OR REPLACE TABLE retaildb.bronze_sch.raw_customer_addresses (
    address_id STRING,
    customer_id STRING,
    address STRING,
    city STRING,
    state STRING,
    zip_code STRING,
    country STRING,

    -- METADATA
    _stg_file_name STRING,                -- Staging file name
    _stg_file_load_ts TIMESTAMP,          -- File load timestamp
    _stg_file_md5 STRING,                 -- File MD5 hash
    _copy_data_ts TIMESTAMP               -- Data copy timestamp
);


-- ###### COPY INTO ######
COPY INTO retaildb.bronze_sch.raw_customer_addresses
FROM (
    SELECT 
        t.$1 AS address_id,
        t.$2 AS customer_id,
        t.$3 AS address,
        t.$4 AS city,
        t.$5 AS state,
        t.$6 AS zip_code,
        t.$7 AS country,
        
        -- Metadata for audit
        metadata$filename as _stg_file_name,
        CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', metadata$file_last_modified) as _stg_file_load_ts,  -- Convert to IST
        metadata$file_content_key as _stg_file_md5,
        CURRENT_TIMESTAMP() as _copy_data_ts                       -- Convert current timestamp to IST
    FROM 
        @retaildb.bronze_sch.csv_stg/data/customer_addresses.csv t
)
FILE_FORMAT = (FORMAT_NAME = 'retaildb.bronze_sch.csv_ff')
ON_ERROR = ABORT_STATEMENT;

SELECT * FROM retaildb.bronze_sch.raw_customer_addresses;




-- ####################################################################################
-- CREATE clean_customer_addresses table INTO SILVER LAYER TO STORE CLEAN DATA
-- ####################################################################################

CREATE OR REPLACE TABLE retaildb.silver_sch.clean_customer_addresses (
    address_id NUMBER AUTOINCREMENT PRIMARY KEY,
    customer_id NUMBER NOT NULL,
    address STRING NOT NULL,
    city STRING NOT NULL,
    state STRING NOT NULL,
    zip_code STRING NOT NULL,
    country STRING NOT NULL,
    
    -- Metadata for SCD
    last_updated_ts TIMESTAMP NOT NULL,           -- Timestamp of the last update
    is_active STRING DEFAULT 'yes',               -- Record status: 'active' or 'inactive'
    
    -- Metadata from Bronze Layer
    _stg_file_name STRING,                        -- Staging file name
    _stg_file_load_ts TIMESTAMP,                  -- File load timestamp
    _stg_file_md5 STRING,                         -- File MD5 hash
    _copy_data_ts TIMESTAMP                       -- Data copy timestamp
);



-- ###### MERGER INTO ######

-- SCD TYPE 1: Silver layer is not designed to maintain the historical record.
MERGE INTO retaildb.silver_sch.clean_customer_addresses AS target
USING (
    -- Deduplicate data from the raw table
    WITH ranked_customers AS (
        SELECT 
            address_id,
            customer_id,
            address,
            city,
            state,
            zip_code,
            country,
            _stg_file_name,
            _stg_file_load_ts,
            _stg_file_md5,
            _copy_data_ts,
            ROW_NUMBER() OVER (PARTITION BY address_id ORDER BY _stg_file_load_ts DESC) AS row_num
        FROM retaildb.bronze_sch.raw_customer_addresses
    )
    SELECT 
        address_id,
        customer_id,
        address,
        city,
        state,
        zip_code,
        country,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts
    FROM ranked_customers
    WHERE row_num = 1             -- Keep only the latest record for each customer_id
) AS source

-- COMMON COLUMN TO MATCH
ON target.customer_id = source.customer_id
WHEN MATCHED THEN
    -- Update existing records with the latest data
    UPDATE SET
        target.customer_id = source.customer_id,
        target.address = source.address,
        target.city = source.city,
        target.state = source.state,
        target.zip_code = source.zip_code,
        target.country = source.country,
        target.last_updated_ts = CURRENT_TIMESTAMP, -- Set last_updated_ts to the current timestamp
        target._stg_file_name = source._stg_file_name,
        target._stg_file_load_ts = source._stg_file_load_ts,
        target._stg_file_md5 = source._stg_file_md5,
        target._copy_data_ts = source._copy_data_ts
WHEN NOT MATCHED THEN
    -- Insert new records
    INSERT (
        address_id,
        customer_id,
        address,
        city,
        state,
        zip_code,
        country,
        last_updated_ts,
        is_active,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts
    ) VALUES (
        source.address_id,
        source.customer_id,
        source.address,
        source.city,
        source.state,
        source.zip_code,
        source.country,
        --CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', CURRENT_TIMESTAMP),         -- Set the last_updated_ts to current time
        CURRENT_TIMESTAMP(),
        'yes',                  -- Default value for is_active
        source._stg_file_name,
        source._stg_file_load_ts,
        source._stg_file_md5,
        source._copy_data_ts
    );



SELECT * FROM retaildb.silver_sch.clean_customer_addresses;



-- ####################################################################################
-- CREATE dim_customer_addresses table INTO GOLD LAYER TO STORE DIMENSION DATA FROM SILVER
-- ####################################################################################

CREATE OR REPLACE TABLE retaildb.gold_sch.dim_customer_addresses (
    address_id NUMBER AUTOINCREMENT PRIMARY KEY,                -- Business key
    customer_id NUMBER NOT NULL,                                 -- Foreign key referencing dim_customers
    address STRING NOT NULL,
    city STRING NOT NULL,
    state STRING NOT NULL,
    zip_code STRING NOT NULL,
    country STRING NOT NULL,

    -- SCD Type 2 Columns
    effective_start_date TIMESTAMP NOT NULL,                     -- When the record became effective
    effective_end_date TIMESTAMP DEFAULT NULL,                   -- When the record was replaced (NULL for active records)
    is_current BOOLEAN DEFAULT TRUE,                              -- TRUE for active record, FALSE otherwise

    last_updated_ts TIMESTAMP NOT NULL,                           -- Timestamp: helps to know when the last change occurred in the source system

    -- Foreign Key Constraint
    CONSTRAINT fk_customer_id FOREIGN KEY (customer_id) REFERENCES retaildb.gold_sch.dim_customers(customer_id)
);


-- ###### MERGE INTO ######

/*
SCD Type 2 Logic: The Gold Layer applies the SCD Type 2 logic, which involves tracking historical changes by marking records as inactive when changes occur, and inserting new records with a new effective start date.
*/

MERGE INTO gold_sch.dim_customer_addresses AS target
USING (
    -- Directly use the clean data from the Silver layer
    SELECT 
        address_id,
        customer_id,
        address,
        city,
        state,
        zip_code,
        country,
        last_updated_ts
    FROM silver_sch.clean_customer_addresses
) AS source

-- Match on the business key (address_id) and customer_id, and only consider current records in the target table
ON target.address_id = source.address_id AND target.customer_id = source.customer_id AND target.is_current = TRUE

WHEN MATCHED AND (
    -- Check if there are changes in the attributes
    target.address <> source.address OR
    target.city <> source.city OR
    target.state <> source.state OR
    target.zip_code <> source.zip_code OR
    target.country <> source.country
) THEN
    -- Close the current record by setting the end date and is_current flag
    UPDATE SET
        target.effective_end_date = CURRENT_TIMESTAMP,    -- Close the current record
        target.is_current = FALSE,                        -- Mark as not current
        target.last_updated_ts = source.last_updated_ts   -- Last updated timestamp

WHEN NOT MATCHED THEN
    -- Insert new record
    INSERT (
        address_id,
        customer_id,
        address,
        city,
        state,
        zip_code,
        country,
        effective_start_date,
        effective_end_date,
        is_current,
        last_updated_ts
    ) VALUES (
        source.address_id,
        source.customer_id,
        source.address,
        source.city,
        source.state,
        source.zip_code,
        source.country,
        CURRENT_TIMESTAMP(),
        --CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', CURRENT_TIMESTAMP),  -- Effective start date (IST)
        NULL,                                                          -- Effective end date (NULL for active record)
        TRUE,                                                          -- Mark as current
        source.last_updated_ts
    );


SELECT * FROM retaildb.gold_sch.dim_customer_addresses;




