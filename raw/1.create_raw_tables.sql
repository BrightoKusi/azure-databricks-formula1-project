-- Databricks notebook source
-- MAGIC %run "../includes/configuration"
-- MAGIC

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create table for the CSV files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### create Circuits table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits(
  circuitId INT
  , circuitRef STRING
  , name STRING
  , location STRING
  , country STRING
  , lat DOUBLE
  , lng DOUBLE
  , alt INT
  , url STRING
)
USING csv
OPTIONS (path "/mnt/formula1bk/raw/circuits.csv", header true);

-- COMMAND ----------

SELECT * FROM f1_raw.circuits

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### create races table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races(
  raceId INT
  , year INT
  , round INT
  , circuitId INT
  , name STRING
  , date DATE
  , time STRING
  , url STRING
)
USING csv
OPTIONS (path "/mnt/formula1bk/raw/races.csv", header true);

-- COMMAND ----------

select * from f1_raw.races

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create tables for JSON files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create constructors table
-- MAGIC - Single line JSON
-- MAGIC - Simple structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors(
  constructorId INT
  , constructorRef STRING
  , name STRING
  , nationality STRING
  , url STRING
)
USING json
OPTIONS(path '/mnt/formula1bk/raw/constructors.json')

-- COMMAND ----------

SELECT * FROM f1_raw.constructors;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create drivers table
-- MAGIC - Single line json
-- MAGIC - complex structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers(
  driverId INT
  , driverRef STRING
  , number INT
  , code STRING
  , name STRUCT<forename: STRING, surname: STRING>
  , dob DATE
  , nationality STRING
  , url STRING
)
USING json
OPTIONS(path '/mnt/formula1bk/raw/drivers.json')

-- COMMAND ----------

SELECT * FROM f1_raw.drivers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create results table
-- MAGIC - Single line json
-- MAGIC - simple structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results(
  resultId INT
  , raceId INT
  , driverId INT
  , constructorId INT
  , number INT
  , grid INT
  , position INT 
  , positionText STRING
  , positionOrder INT
  , points DOUBLE
  , laps INT
  , time STRING
  , milliseconds INT
  , fastestLap INT
  , rank INT
  , fastestLapTime STRING
  , fastestLapSpeed FLOAT
  , StatusId INT
)
USING json
OPTIONS(path "/mnt/formula1bk/raw/results.json")

-- COMMAND ----------

SELECT * FROM f1_raw.results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create pit_stops table
-- MAGIC - Multiple line json
-- MAGIC - simple structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS  f1_raw.pit_stops(
  driverId INT
  , duration STRING
  , lap INT
  , milliseconds INT
  , raceId INT
  , stop INT
  , time STRING
)
USING json 
OPTIONS(path "/mnt/formula1bk/raw/pit_stops.json", multiLine true)


-- COMMAND ----------

SELECT * FROM f1_raw.pit_stops

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create tables for list of files
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create Lap Times table
-- MAGIC - CSV file
-- MAGIC - Multiple files
-- MAGIC

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
  raceId INT
  , driverId INT
  , lap INT
  , position INT
  , time STRING
  , milliseconds INT
)
USING csv 
OPTIONS(path "/mnt/formula1bk/raw/lap_times")


-- COMMAND ----------

SELECT * FROM f1_raw.lap_times

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
  qualifyId INT
  , raceId INT
  , driverId INT
  , constructorId INT
  , number INT
  , position INT
  , q1 STRING
  , q2 STRING
  , q3 STRING
)
USING json 
OPTIONS(path "/mnt/formula1bk/raw/qualifying", multiLine true)

-- COMMAND ----------

SELECT * FROM f1_raw.qualifying
