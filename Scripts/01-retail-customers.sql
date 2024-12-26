/*
Note: I will be following Medalion Architecture along with Slowly Changing Dimensions(SCD) being applied to dimesnsion tables.
*/

USE DATABASE retaildb;

-- ############### Query data from internal stage files directly ################
-- customer_id,name,gender,email,dob,phone_number,joining_date

SELECT 
    t.$1 AS customer_id,
    t.$2 AS name,
    t.$3 AS gender,
    t.$4 AS email,
    t.$5 AS dob,
    t.$6 AS phone_number,
    t.$7 AS joining_date,
    
    -- Metadata for audit
    metadata$filename as _stg_file_name,
    --metadata$file_last_modified as _stg_file_load_ts,
    CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', metadata$file_last_modified) as _stg_file_load_ts,  -- Convert to IST
    metadata$file_content_key as _stg_file_md5,
    current_timestamp() as _copy_data_ts  -- Convert current timestamp to IST
FROM 
    @retaildb.bronze_sch.csv_stg/data/customers.csv 
(FILE_FORMAT => bronze_sch.csv_ff) t;




-- ####################################################################################
-- CREATE raw_customer table INTO BRONZE LAYER TO STORE SOURCE DATA AS-IS
-- ####################################################################################

-- customer_id,name,gender,email,dob,phone_number,joining_date
CREATE OR REPLACE TABLE retaildb.bronze_sch.raw_customers (
    customer_id STRING,
    name STRING,
    gender STRING,
    email STRING,
    dob STRING,
    phone_number STRING, 
    joining_date STRING, 

    -- METADATA
    _stg_file_name STRING,                -- Staging file name
    _stg_file_load_ts TIMESTAMP,          -- File load timestamp
    _stg_file_md5 STRING,                 -- File MD5 hash
    _copy_data_ts TIMESTAMP               -- Data copy timestamp
);

SELECT * FROM retaildb.bronze_sch.raw_customers;


-- ###### COPY INTO ######
COPY INTO retaildb.bronze_sch.raw_customers
FROM (
    SELECT 
        t.$1 AS customer_id,
        t.$2 AS name,
        t.$3 AS gender,
        t.$4 AS email,
        t.$5 AS dob,
        t.$6 AS phone_number,
        t.$7 AS joining_date,
        
        -- Metadata for audit
        metadata$filename as _stg_file_name,
        --metadata$file_last_modified as _stg_file_load_ts,
        CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', metadata$file_last_modified) as _stg_file_load_ts,  -- Convert to IST
        metadata$file_content_key as _stg_file_md5,
        CURRENT_TIMESTAMP() as _copy_data_ts                       -- Convert current timestamp to IST
    FROM 
        @retaildb.bronze_sch.csv_stg/data/customers.csv t
)
FILE_FORMAT = (FORMAT_NAME = 'retaildb.bronze_sch.csv_ff')
ON_ERROR = ABORT_STATEMENT;

SELECT * FROM retaildb.bronze_sch.raw_customers;


-- ####################################################################################
-- CREATE clean_customers table INTO SILVER LAYER TO STORE CLEANED DATA FROM BRONZE
-- ####################################################################################


CREATE OR REPLACE TABLE retaildb.silver_sch.clean_customers (
    customer_id NUMBER AUTOINCREMENT PRIMARY KEY,
    name STRING NOT NULL,
    gender STRING NOT NULL,
    email STRING NOT NULL,
    dob DATE NOT NULL,
    phone_number STRING NOT NULL,
    joining_date DATE NOT NULL,
    
    -- Metadata for SCD
    last_updated_ts TIMESTAMP NOT NULL,           -- Timestamp of the last update
    is_active STRING DEFAULT 'yes',            -- Record status: 'active' or 'inactive'
    
    -- Metadata from Bronze Layer
    _stg_file_name STRING,                        -- Staging file name
    _stg_file_load_ts TIMESTAMP,                  -- File load timestamp
    _stg_file_md5 STRING,                         -- File MD5 hash
    _copy_data_ts TIMESTAMP                       -- Data copy timestamp
);

-- ###### MERGER INTO ######

-- SCD TYPE 1: Silver layer is not designed to maintain the historical record.
MERGE INTO silver_sch.clean_customers AS target
USING (
    -- Deduplicate data from the raw table
    WITH ranked_customers AS (
        SELECT 
            customer_id,
            name,
            gender,
            email,
            dob,
            phone_number,
            joining_date,
            _stg_file_name,
            _stg_file_load_ts,
            _stg_file_md5,
            _copy_data_ts,
            ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY _stg_file_load_ts DESC) AS row_num
        FROM bronze_sch.raw_customers
    )
    SELECT 
        customer_id,
        name,
        gender,
        email,
        dob,
        phone_number,
        joining_date,
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
        target.name = source.name,
        target.gender = source.gender,
        target.email = source.email,
        target.dob = source.dob,
        target.phone_number = source.phone_number,
        target.joining_date = source.joining_date,
        target.last_updated_ts = CURRENT_TIMESTAMP, -- Set to the current timestamp
        target._stg_file_name = source._stg_file_name,
        target._stg_file_load_ts = source._stg_file_load_ts,
        target._stg_file_md5 = source._stg_file_md5,
        target._copy_data_ts = source._copy_data_ts
WHEN NOT MATCHED THEN
    -- Insert new records
    INSERT (
        customer_id,
        name,
        gender,
        email,
        dob,
        phone_number,
        joining_date,
        last_updated_ts,
        is_active,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts
    ) VALUES (
        source.customer_id,
        source.name,
        source.gender,
        source.email,
        source.dob,
        source.phone_number,
        source.joining_date,
        --CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', CURRENT_TIMESTAMP),         -- Set the last_updated_ts to current time
        CURRENT_TIMESTAMP(),
        'yes',                  -- Default value for is_active
        source._stg_file_name,
        source._stg_file_load_ts,
        source._stg_file_md5,
        source._copy_data_ts
    );
    
SELECT * FROM retaildb.silver_sch.clean_customers;



-- ####################################################################################
-- CREATE dim_customers table INTO GOLD LAYER TO STORE DIMENSION DATA FROM SILVER
-- ####################################################################################

CREATE OR REPLACE TABLE retaildb.gold_sch.dim_customers (
    customer_id NUMBER AUTOINCREMENT PRIMARY KEY,                 -- Business key
    name STRING NOT NULL,
    gender STRING NOT NULL,
    email STRING NOT NULL,
    dob DATE NOT NULL,
    phone_number STRING NOT NULL,
    joining_date DATE NOT NULL,

    -- SCD Type 2 Columns
    effective_start_date TIMESTAMP NOT NULL,     -- When the record became effective
    effective_end_date TIMESTAMP DEFAULT NULL,   -- When the record was replaced (NULL for active records)
    is_current BOOLEAN DEFAULT TRUE,             -- TRUE for active record, FALSE otherwise

    last_updated_ts TIMESTAMP NOT NULL          -- Timestamp: helps to know when the last change occurred in the source system.
);


-- ###### MERGER INTO ######

/*
SCD Type 2 Logic: The Gold Layer applies the SCD Type 2 logic, which involves tracking historical changes by marking records as inactive when changes occur, and inserting new records with a new effective start date.
*/

MERGE INTO gold_sch.dim_customers AS target
USING (
    -- Directly use the clean data from the Silver layer
    SELECT 
        customer_id,
        name,
        gender,
        email,
        dob,
        phone_number,
        joining_date,
        last_updated_ts

    FROM silver_sch.clean_customers
) AS source

-- Match on the business key (customer_id) and only consider current records in the target table
ON target.customer_id = source.customer_id AND target.is_current = TRUE

WHEN MATCHED AND (
    -- Check if there are changes in the attributes
    target.name <> source.name OR
    target.gender <> source.gender OR
    target.email <> source.email OR
    target.dob <> source.dob OR
    target.phone_number <> source.phone_number OR
    target.joining_date <> source.joining_date
) THEN
    -- Close the current record by setting the end date and is_current flag
    UPDATE SET
        target.effective_end_date = CURRENT_TIMESTAMP,   -- Close the current record
        target.is_current = FALSE,                       -- Mark as not current
        target.last_updated_ts = source.last_updated_ts  -- Last updated timestamp

WHEN NOT MATCHED THEN
    -- Insert new record
    INSERT (
        customer_id,
        name,
        gender,
        email,
        dob,
        phone_number,
        joining_date,
        effective_start_date,
        effective_end_date,
        is_current,
        last_updated_ts
    ) VALUES (
        source.customer_id,
        source.name,
        source.gender,
        source.email,
        source.dob,
        source.phone_number,
        source.joining_date,
        CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', CURRENT_TIMESTAMP),     -- Effective start date (IST)
        NULL,                                                           -- Effective end date (NULL for active record)
        TRUE,                                                           -- Mark as current
        source.last_updated_ts
    );


    SELECT * FROM retaildb.gold_sch.dim_customers;










