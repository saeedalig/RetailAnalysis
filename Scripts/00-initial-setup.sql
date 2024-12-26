-- Set the default session time zone for the entire account
ALTER ACCOUNT SET TIMEZONE = 'Asia/Kolkata';

-- use sysadmin role.
USE ROLE sysadmin;


-- Create warehouse
CREATE OR ALTER WAREHOUSE retail_wh
  WAREHOUSE_TYPE = 'STANDARD'
  WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_RESUME = TRUE
  COMMENT = 'Warehouse that will be used for retail project';

-- Create database
CREATE DATABASE IF NOT EXISTS retaildb;
USE retaildb;

-- Create schema
CREATE SCHEMA IF NOT EXISTS bronze_sch;
CREATE SCHEMA IF NOT EXISTS silver_sch;
CREATE SCHEMA IF NOT EXISTS gold_sch;
CREATE SCHEMA IF NOT EXISTS common_sch;


-- Use database
USE DATABASE retaildb;


-- Create file format for csv data 
CREATE OR REPLACE FILE FORMAT bronze_sch.csv_ff
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '\042'
    NULL_IF = ('NULL', 'null', '\\N')
    EMPTY_FIELD_AS_NULL = true
    COMPRESSION = 'AUTO';

-- Create stage with directorty enabled
CREATE OR REPLACE STAGE bronze_sch.csv_stg
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = "internal stage created to store csv data" ;

show stages;

-- Dump the data files to internal stage via PUT commonads or Snowsight

-- List the files available into stage
LIST @retaildb.bronze_sch.csv_stg/data;






