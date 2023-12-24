-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Set the storage account variable from a KV secret

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbx_kv_scope = 'dbx_kv_scope_01'
-- MAGIC stg_kv_name = 'clientdemo-stg-account-base-abfss-path'
-- MAGIC stg_base_uri = dbutils.secrets.get(scope=dbx_kv_scope, key=stg_kv_name)
-- MAGIC
-- MAGIC # Set the Python variable as a Spark configuration property
-- MAGIC spark.conf.set("spark.databricks.stgBaseUri", stg_base_uri)
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Copy Flights Data into Table (Extract from Storage & Load into a table)

-- COMMAND ----------

-- Ensure the unity catalog is created for 'workshop'
CREATE CATALOG IF NOT EXISTS demos;
  USE CATALOG demos;
  
-- Ensure the schema/database for is created
CREATE SCHEMA IF NOT EXISTS flights;
  USE SCHEMA flights;

-- Set enterprise wide SELECT permissions to all tables in this schema
GRANT USE_CATALOG ON CATALOG demos TO `account users`;
GRANT USE_SCHEMA ON SCHEMA demos.flights TO `account users`;
GRANT SELECT ON SCHEMA demos.flights TO `account users`;

-- COMMAND ----------

-- Create the empty delta table
CREATE TABLE IF NOT EXISTS flights_raw (
        Year INT,
        FlightDate DATE,
        Reporting_Airline STRING,
        DOT_ID_Reporting_Airline STRING,
        IATA_CODE_Reporting_Airline STRING,
        Flight_Number_Reporting_Airline STRING,
        OriginAirportID STRING,
        OriginCityName STRING,
        OriginState STRING,
        DestAirportID STRING,
        DestCityName STRING,
        DestState STRING,
        DepTime INT,
        DepDelay DOUBLE,
        DepDelayMinutes DOUBLE,
        ArrTime INT,
        ArrDelay DOUBLE,
        ArrDelayMinutes DOUBLE,
        Distance DOUBLE,
        CarrierDelay DOUBLE,
        WeatherDelay DOUBLE,
        NASDelay DOUBLE,
        SecurityDelay DOUBLE
        );

-- COMMAND ----------

-- Copy files (and new files as they arrive) into the table
-- COPY INTO will only process new CSV records since the last process
COPY INTO flights_raw
  FROM 
    ( 
      SELECT
        INT(Year),
        DATE(FlightDate),
        Reporting_Airline,
        DOT_ID_Reporting_Airline,
        IATA_CODE_Reporting_Airline,
        Flight_Number_Reporting_Airline,
        OriginAirportID,
        OriginCityName,
        OriginState,
        DestAirportID,
        DestCityName,
        DestState,
        INT(DepTime),
        DOUBLE(DepDelay),
        DOUBLE(DepDelayMinutes),
        INT(ArrTime),
        DOUBLE(ArrDelay),
        DOUBLE(ArrDelayMinutes),
        DOUBLE(Distance),
        DOUBLE(CarrierDelay),
        DOUBLE(WeatherDelay),
        DOUBLE(NASDelay),
        DOUBLE(SecurityDelay)
      FROM '${spark.databricks.stgBaseUri}/flights_data/flights'
      )
  FILEFORMAT = CSV
  FORMAT_OPTIONS ('mergeSchema' = 'true',
                  'header' = 'true')
  COPY_OPTIONS ('mergeSchema' = 'true');
  
--SELECT count(*) FROM flights_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Copy Airport Data into Table

-- COMMAND ----------

-- -- Create the Airline table

CREATE OR REPLACE TEMPORARY VIEW airline_load_temp
  USING csv
  OPTIONS (path '${spark.databricks.stgBaseUri}/flights_data/dims/carriers.csv',
    header 'true');

CREATE OR REPLACE TABLE flights_airline_raw AS (
  SELECT * FROM airline_load_temp);

-- --SELECT * FROM flights_airline;

-- Create the Airports table
CREATE OR REPLACE TEMPORARY VIEW airport_load_temp
  USING csv
  OPTIONS (path '${spark.databricks.stgBaseUri}/flights_data/dims/airports.csv',
    header 'true');

CREATE OR REPLACE TABLE flights_airport_raw AS (
  SELECT * FROM airport_load_temp);

--SELECT * FROM flights_airport;


-- COMMAND ----------

-- MAGIC %md 
-- MAGIC
-- MAGIC ## Create Table for Analysis

-- COMMAND ----------

CREATE OR REPLACE TABLE flights_on_time_perf_clean
PARTITIONED BY (Year)
AS 
SELECT
 Fl.FlightDate, 
 YEAR(FL.FlightDate) as Year,
 Fl.OriginCityName, 
 Fl.OriginState, 
 Fl.DestCityName, 
 Fl.DestState, 
 Fl.ArrDelayMinutes, 
 Fl.DepDelayMinutes,
 SPLIT_PART(Al.Carrier, ":", 1) as Airline
FROM
 flights_raw fl
   LEFT JOIN flights_airline_raw Al ON fl.Reporting_Airline = Al.ID;

--SELECT * FROM flights_on_time_perf_clean;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Optimize

-- COMMAND ----------

OPTIMIZE flights_on_time_perf_clean ZORDER BY (Airline);
