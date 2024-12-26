USE DATABASE retaildb;
USE SCHEMA retaildb.bronze_sch ;

SELECT CURRENT_TIMESTAMP();


LIST @retaildb.bronze_sch.csv_stg/data/locations.csv;

-- location_id,city,state,country,latitude,longitude

SELECT 
    t.$1 AS location_id,
    t.$2 AS city,
    t.$3 AS state,
    t.$4 AS country,
    t.$5 AS latitude,
    t.$6 AS longitude,
    
    -- Metadata for audit
    metadata$filename as _stg_file_name,
    CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', metadata$file_last_modified) as _stg_file_load_ts,  -- Convert to IST
    metadata$file_content_key as _stg_file_md5,
    CURRENT_TIMESTAMP() as _copy_data_ts                       -- Convert current timestamp to IST
FROM @retaildb.bronze_sch.csv_stg/data/locations.csv 
(FILE_FORMAT => retaildb.bronze_sch.csv_ff) t;


-- ####################################################################################
-- CREATE raw_locations table INTO BRONZE LAYER TO STORE SOURCE DATA AS-IS
-- ####################################################################################

-- location_id,city,state,country,latitude,longitude
CREATE OR REPLACE TABLE retaildb.bronze_sch.raw_locations (
    location_id STRING,
    city STRING,
    state STRING,
    country STRING,
    latitude STRING,
    longitude STRING,

    -- METADATA
    _stg_file_name STRING,                -- Staging file name
    _stg_file_load_ts TIMESTAMP,          -- File load timestamp
    _stg_file_md5 STRING,                 -- File MD5 hash
    _copy_data_ts TIMESTAMP               -- Data copy timestamp
);


-- ###### COPY INTO ######
COPY INTO retaildb.bronze_sch.raw_locations
FROM (
    SELECT 
        t.$1 AS location_id,
        t.$2 AS city,
        t.$3 AS state,
        t.$4 AS country,
        t.$5 AS latitude,
        t.$6 AS longitude,
            
        -- Metadata for audit
        metadata$filename as _stg_file_name,
        CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', metadata$file_last_modified) as _stg_file_load_ts,  -- Convert to IST
        metadata$file_content_key as _stg_file_md5,
        CURRENT_TIMESTAMP() as _copy_data_ts                       -- Convert current timestamp to IST
    FROM @retaildb.bronze_sch.csv_stg/data/locations.csv t
)
FILE_FORMAT = (FORMAT_NAME = 'retaildb.bronze_sch.csv_ff')
ON_ERROR = ABORT_STATEMENT;

SELECT * FROM retaildb.bronze_sch.raw_locations;



-- ####################################################################################
-- CREATE clean_locations table INTO SILVER LAYER TO STORE CLEAN DATA
-- ####################################################################################
DROP TABLE retaildb.silver_sch.clean_locations;
CREATE OR REPLACE TABLE retaildb.silver_sch.clean_locations (
    location_id NUMBER AUTOINCREMENT PRIMARY KEY,
    city STRING NOT NULL,
    state STRING NOT NULL,
    country STRING NOT NULL,
    latitude STRING NOT NULL,
    longitude STRING NOT NULL,
    
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
MERGE INTO retaildb.silver_sch.clean_locations AS target
USING (
    -- Deduplicate data from the raw table
    WITH ranked_locations AS (
        SELECT 
            location_id,
            city,
            state,
            country,
            latitude,
            longitude,
            _stg_file_name,
            _stg_file_load_ts,
            _stg_file_md5,
            _copy_data_ts,
            ROW_NUMBER() OVER (PARTITION BY location_id ORDER BY _stg_file_load_ts DESC) AS row_num
        FROM retaildb.bronze_sch.raw_locations
    )
    SELECT 
        location_id,
        city,
        state,
        country,
        latitude,
        longitude,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts
    FROM ranked_locations
    WHERE row_num = 1             -- Keep only the latest record for each location_id
) AS source

-- COMMON COLUMN TO MATCH
ON target.location_id = source.location_id
WHEN MATCHED THEN
    -- Update existing records with the latest data
    UPDATE SET
        target.city = source.city,
        target.state = source.state,
        target.country = source.country,
        target.latitude = source.latitude,
        target.longitude = source.longitude,
        target.last_updated_ts = CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', CURRENT_TIMESTAMP), -- Set last_updated_ts to the current timestamp
        target._stg_file_name = source._stg_file_name,
        target._stg_file_load_ts = source._stg_file_load_ts,
        target._stg_file_md5 = source._stg_file_md5,
        target._copy_data_ts = source._copy_data_ts
WHEN NOT MATCHED THEN
    -- Insert new records
    INSERT (
        location_id,
        city,
        state,
        country,
        latitude,
        longitude,
        last_updated_ts,
        is_active,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts
    ) VALUES (
        source.location_id,
        source.city,
        source.state,
        source.country,
        source.latitude,
        source.longitude,
        CURRENT_TIMESTAMP,
        --CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', CURRENT_TIMESTAMP),         -- Set the last_updated_ts to current time
        'yes',                  -- Default value for is_active
        source._stg_file_name,
        source._stg_file_load_ts,
        source._stg_file_md5,
        source._copy_data_ts
    );

SELECT * FROM retaildb.silver_sch.clean_locations;


    
-- ####################################################################################
-- CREATE dim_locations table INTO GOLD LAYER TO STORE DIMENSION DATA FROM SILVER
-- ####################################################################################

CREATE OR REPLACE TABLE retaildb.gold_sch.dim_locations (
    location_id NUMBER AUTOINCREMENT PRIMARY KEY,                              -- Business key
    city STRING NOT NULL,
    state STRING NOT NULL,
    country STRING NOT NULL,
    latitude STRING NOT NULL,
    longitude STRING NOT NULL,

    -- SCD Type 2 Columns
    effective_start_date TIMESTAMP NOT NULL,                     -- When the record became effective
    effective_end_date TIMESTAMP DEFAULT NULL,                   -- When the record was replaced (NULL for active records)
    is_current BOOLEAN DEFAULT TRUE,                              -- TRUE for active record, FALSE otherwise

    last_updated_ts TIMESTAMP NOT NULL                           -- Timestamp: helps to know when the last change occurred in the source system
);



-- ###### MERGE INTO ######

/*
SCD Type 2 Logic: The Gold Layer applies the SCD Type 2 logic, which involves tracking historical changes by marking records as inactive when changes occur, and inserting new records with a new effective start date.
*/

MERGE INTO gold_sch.dim_locations AS target
USING (
    -- Directly use the clean data from the Silver layer
    SELECT 
        location_id,
        city,
        state,
        country,
        latitude,
        longitude,
        last_updated_ts
    FROM silver_sch.clean_locations
) AS source

-- Match on the business key (location_id) and only consider current records in the target table
ON target.location_id = source.location_id AND target.is_current = TRUE

WHEN MATCHED AND (
    -- Check if there are changes in the attributes
    target.city <> source.city OR
    target.state <> source.state OR
    target.country <> source.country OR
    target.latitude <> source.latitude OR
    target.longitude <> source.longitude
) THEN
    -- Close the current record by setting the end date and is_current flag
    UPDATE SET
        target.effective_end_date = CURRENT_TIMESTAMP,    -- Close the current record
        target.is_current = FALSE,                        -- Mark as not current
        target.last_updated_ts = source.last_updated_ts   -- Last updated timestamp

WHEN NOT MATCHED THEN
    -- Insert new record
    INSERT (
        location_id,
        city,
        state,
        country,
        latitude,
        longitude,
        effective_start_date,
        effective_end_date,
        is_current,
        last_updated_ts
    ) VALUES (
        source.location_id,
        source.city,
        source.state,
        source.country,
        source.latitude,
        source.longitude,
        CURRENT_TIMESTAMP,
        --CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', CURRENT_TIMESTAMP),   -- Effective start date (current time)
        NULL,                  -- Effective end date (NULL for active record)
        TRUE,                  -- Mark as current
        source.last_updated_ts
    );


SELECT * FROM retaildb.gold_sch.dim_locations;
    
    