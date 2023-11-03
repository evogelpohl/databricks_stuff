-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ## Copy Flights into Table

-- COMMAND ----------

-- Ensure the unity catalog is created for 'workshop'
CREATE CATALOG IF NOT EXISTS workshop;
  USE CATALOG workshop;
  
-- Ensure the schema/database for is created
CREATE SCHEMA IF NOT EXISTS data;
  USE SCHEMA data;
  
-- DROP TABLE for testing only
--DROP TABLE IF EXISTS flights_raw;

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
      FROM 'abfss://data@bpworkshopstg.dfs.core.windows.net/raw_data/flights_data/'
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

-- Create the Airline table

CREATE OR REPLACE TEMPORARY VIEW airline_load_temp
  USING csv
  OPTIONS (path 'abfss://data@bpworkshopstg.dfs.core.windows.net/raw_data/flights_data_dim_tables/airlines/L_AIRLINE_ID.csv',
    header 'true');

CREATE OR REPLACE TABLE flights_airline_raw AS (
  SELECT * FROM airline_load_temp);

--SELECT * FROM flights_airline;

-- Create the Airports table
CREATE OR REPLACE TEMPORARY VIEW airport_load_temp
  USING csv
  OPTIONS (path 'abfss://data@bpworkshopstg.dfs.core.windows.net/raw_data/flights_data_dim_tables/airports/L_AIRPORT_ID.csv',
    header 'true');

CREATE OR REPLACE TABLE flights_airport_raw AS (
  SELECT * FROM airport_load_temp);

--SELECT * FROM flights_airport;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Optimize

-- COMMAND ----------

--OPTIMIZE flights_raw ZORDER BY Year;
OPTIMIZE flights_airline_raw;
OPTIMIZE flights_airport_raw;

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
 Fl.Year,
 Fl.OriginCityName, 
 Fl.OriginState, 
 Fl.DestCityName, 
 Fl.DestState, 
 Fl.ArrDelayMinutes, 
 Fl.DepDelayMinutes,
 SPLIT_PART(Al.Description, ":", 1) as Airline
FROM
 workshop.data.flights_raw fl
   LEFT JOIN workshop.data.flights_airline_raw Al ON fl.DOT_ID_Reporting_Airline = Al.Code;


-- COMMAND ----------

OPTIMIZE workshop.data.flights_on_time_perf_clean ZORDER BY (Year);

ANALYZE TABLE workshop.data.flights_on_time_perf_clean COMPUTE STATISTICS FOR ALL COLUMNS;
