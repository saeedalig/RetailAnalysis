USE DATABASE retaildb;


-- ####################################################################################
--  QUERY DIRECTLY FROM INTERNAL STAGE FOR delivery_partner
-- ####################################################################################

SELECT 
    t.$1 AS delivery_partner_id,
    t.$2 AS partner_name,
    t.$3 AS contact_number,
    t.$4 AS service_area,
    
    -- Metadata for audit
    metadata$filename as _stg_file_name,
    CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', metadata$file_last_modified) as _stg_file_load_ts,  -- Convert to IST
    metadata$file_content_key as _stg_file_md5,
    CURRENT_TIMESTAMP() as _copy_data_ts                       -- Current timestamp
FROM 
    @retaildb.bronze_sch.csv_stg/data/delivery_partners.csv 
    (FILE_FORMAT => retaildb.bronze_sch.csv_ff) t;



-- ####################################################################################
-- CREATE raw_delivery_partner table INTO BRONZE LAYER TO STORE SOURCE DATA AS-IS
-- ####################################################################################

-- delivery_partner_id, partner_name, contact_number, service_area
CREATE OR REPLACE TABLE retaildb.bronze_sch.raw_delivery_partners (
    delivery_partner_id STRING,
    partner_name STRING,
    contact_number STRING,
    service_area STRING,

    -- METADATA
    _stg_file_name STRING,                -- Staging file name
    _stg_file_load_ts TIMESTAMP,          -- File load timestamp
    _stg_file_md5 STRING,                 -- File MD5 hash
    _copy_data_ts TIMESTAMP               -- Data copy timestamp
);

-- ###### COPY INTO ######
COPY INTO retaildb.bronze_sch.raw_delivery_partners
FROM (
    SELECT 
        t.$1 AS delivery_partner_id,
        t.$2 AS partner_name,
        t.$3 AS contact_number,
        t.$4 AS service_area,
            
        -- Metadata for audit
        metadata$filename as _stg_file_name,
        CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', metadata$file_last_modified) as _stg_file_load_ts,  -- Convert to IST
        metadata$file_content_key as _stg_file_md5,
        CURRENT_TIMESTAMP() as _copy_data_ts                       -- Convert current timestamp to IST
    FROM @retaildb.bronze_sch.csv_stg/data/delivery_partners.csv t
)
FILE_FORMAT = (FORMAT_NAME = 'retaildb.bronze_sch.csv_ff')
ON_ERROR = ABORT_STATEMENT;

SELECT * FROM retaildb.bronze_sch.raw_delivery_partners;



-- ####################################################################################
-- CREATE clean_delivery_partners table INTO SILVER LAYER TO STORE CLEAN DATA
-- ####################################################################################

CREATE OR REPLACE TABLE retaildb.silver_sch.clean_delivery_partners (
    delivery_partner_id NUMBER AUTOINCREMENT PRIMARY KEY,
    partner_name STRING NOT NULL,
    contact_number STRING NOT NULL,
    service_area STRING NOT NULL,

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
MERGE INTO retaildb.silver_sch.clean_delivery_partners AS target
USING (
    -- Deduplicate data from the raw table
    WITH ranked_partners AS (
        SELECT 
            delivery_partner_id,
            partner_name,
            contact_number,
            service_area,
            _stg_file_name,
            _stg_file_load_ts,
            _stg_file_md5,
            _copy_data_ts,
            ROW_NUMBER() OVER (PARTITION BY delivery_partner_id ORDER BY _stg_file_load_ts DESC) AS row_num
        FROM retaildb.bronze_sch.raw_delivery_partners
    )
    SELECT 
        delivery_partner_id,
        partner_name,
        contact_number,
        service_area,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts
    FROM ranked_partners
    WHERE row_num = 1             -- Keep only the latest record for each delivery_partner_id
) AS source

-- COMMON COLUMN TO MATCH
ON target.delivery_partner_id = source.delivery_partner_id
WHEN MATCHED THEN
    -- Update existing records with the latest data
    UPDATE SET
        target.partner_name = source.partner_name,
        target.contact_number = source.contact_number,
        target.service_area = source.service_area,
        target.last_updated_ts = CURRENT_TIMESTAMP, -- Set last_updated_ts to the current timestamp
        target._stg_file_name = source._stg_file_name,
        target._stg_file_load_ts = source._stg_file_load_ts,
        target._stg_file_md5 = source._stg_file_md5,
        target._copy_data_ts = source._copy_data_ts
WHEN NOT MATCHED THEN
    -- Insert new records
    INSERT (
        delivery_partner_id,
        partner_name,
        contact_number,
        service_area,
        last_updated_ts,
        is_active,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts
    ) VALUES (
        source.delivery_partner_id,
        source.partner_name,
        source.contact_number,
        source.service_area,
        --CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', CURRENT_TIMESTAMP),         -- Set the last_updated_ts to current time
        CURRENT_TIMESTAMP(),
        'yes',                  -- Default value for is_active
        source._stg_file_name,
        source._stg_file_load_ts,
        source._stg_file_md5,
        source._copy_data_ts
    );

-- View the cleaned data
SELECT * FROM retaildb.silver_sch.clean_delivery_partners;



-- ####################################################################################
-- CREATE dim_delivery_partners TABLE INTO GOLD LAYER TO STORE DIMENSION DATA FROM SILVER
-- ####################################################################################

CREATE OR REPLACE TABLE retaildb.gold_sch.dim_delivery_partners (
    delivery_partner_id NUMBER AUTOINCREMENT PRIMARY KEY,                  -- Business key
    partner_name STRING NOT NULL,                                 -- Name of the delivery partner
    contact_number STRING NOT NULL,                               -- Contact number of the partner
    service_area STRING NOT NULL,

    -- SCD Type 2 Columns
    effective_start_date TIMESTAMP NOT NULL,                      -- When the record became effective
    effective_end_date TIMESTAMP DEFAULT NULL,                    -- When the record was replaced (NULL for active records)
    is_current BOOLEAN DEFAULT TRUE,                              -- TRUE for active record, FALSE otherwise

    last_updated_ts TIMESTAMP NOT NULL                            -- Timestamp: helps to know when the last change occurred in the source system
);


-- ###### MERGE INTO ######

/*
SCD Type 2 Logic: Tracks historical changes for delivery partners by marking records as inactive when changes occur, and inserting new records with a new effective start date.
*/

MERGE INTO gold_sch.dim_delivery_partners AS target
USING (
    -- Select clean data from the Silver Layer
    SELECT 
        delivery_partner_id,
        partner_name,
        contact_number,
        service_area,
        last_updated_ts
    FROM silver_sch.clean_delivery_partners
) AS source

-- Match on the business key (partner_id) and consider only current records in the target table
ON target.delivery_partner_id = source.delivery_partner_id AND target.is_current = TRUE

WHEN MATCHED AND (
    -- Check if there are changes in the attributes
    target.partner_name <> source.partner_name OR
    target.contact_number <> source.contact_number OR
    target.service_area <> source.service_area
) THEN
    -- Close the current record by setting the end date and is_current flag
    UPDATE SET
        target.effective_end_date = CURRENT_TIMESTAMP,    -- Close the current record
        target.is_current = FALSE,                        -- Mark as not current
        target.last_updated_ts = source.last_updated_ts   -- Last updated timestamp

WHEN NOT MATCHED THEN
    -- Insert new record
    INSERT (
        delivery_partner_id,
        partner_name,
        contact_number,
        service_area,
        effective_start_date,
        effective_end_date,
        is_current,
        last_updated_ts
    ) VALUES (
        source.delivery_partner_id,
        source.partner_name,
        source.contact_number,
        source.service_area,
        CURRENT_TIMESTAMP(),
        --CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', CURRENT_TIMESTAMP),  -- Effective start date (IST)
        NULL,                                                          -- Effective end date (NULL for active record)
        TRUE,                                                          -- Mark as current
        source.last_updated_ts
    );

SELECT * FROM retaildb.gold_sch.dim_delivery_partners;



    