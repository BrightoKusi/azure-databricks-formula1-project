-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###Learning objectives
-- MAGIC 1. Spark SQL documentation
-- MAGIC 2. Create Database demo
-- MAGIC 3. Data tab in the UI
-- MAGIC 4. SHOW command
-- MAGIC 5. DESCRIBE command
-- MAGIC 6. Find the current database
-- MAGIC

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

DESCRIBE DATABASE demo;

-- COMMAND ----------

DESCRIBE  DATABASE EXTENDED demo;

-- COMMAND ----------

SELECT current_database();

-- COMMAND ----------

SHOW TABLES IN default;

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Managed tables
-- MAGIC 1. Create a managed table using Python
-- MAGIC 2. Create a managed table using SQL
-- MAGIC 3. Effect of dropping a managed table
-- MAGIC 4. Describe table

-- COMMAND ----------

-- MAGIC
-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

USE demo;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f'{presentation_folder_path}/race_results')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #write to table
-- MAGIC race_results_df.write.mode('overwrite').format('parquet').saveAsTable('demo_race_results_python')

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

DESC EXTENDED demo_race_results_python;

-- COMMAND ----------

SELECT * 
FROM demo.demo_race_results_python
WHERE race_year = 2020;

-- COMMAND ----------

CREATE TABLE demo.race_results_sql
AS 
SELECT * 
  FROM demo.demo_race_results_python
  WHERE race_year = 2020

-- COMMAND ----------

DESC EXTENDED race_results_sql;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### External tables
-- MAGIC 1. Create a external table using Python
-- MAGIC 2. Create a external table using SQL
-- MAGIC 3. Effect of dropping a external table
-- MAGIC 4. Describe table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format('parquet').option("path", f'{presentation_folder_path}/race_results_ext_py').saveAsTable('demo_race_results_ext_py')

-- COMMAND ----------

DESC EXTENDED demo_race_results_ext_py;

-- COMMAND ----------

CREATE TABLE demo.race_results_ext_sql(
  race_year INT
  , race_name STRING
  , race_date TIMESTAMP
  , circuit_location STRING
  , driver_name STRING
  , driver_number INT
  , driver_nationality STRING
  , team STRING
  , grid INT
  , fastest_lap INT
  , race_time STRING
  , points FLOAT
  , position INT
  , created_date TIMESTAMP
)
USING parquet 
LOCATION '/mnt/formula1bk/presentation/race_results_ext_sql'
;

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

INSERT INTO demo.race_results_ext_sql
SELECT * FROM demo.demo_race_results_ext_py WHERE race_year = 2020;

-- COMMAND ----------

SELECT * FROM demo.race_results_ext_sql
