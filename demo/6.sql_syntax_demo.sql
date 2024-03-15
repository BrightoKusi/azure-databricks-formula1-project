-- Databricks notebook source
SHOW DATABASES;

-- COMMAND ----------

USE f1_processed;

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

SELECT * 
FROM f1_processed.drivers
WHERE nationality = 'British'
LIMIT 10

-- COMMAND ----------

SELECT *, CONCAT(driver_ref, ' ', code) as dc
FROM f1_processed.drivers;


-- COMMAND ----------

SELECT SPLIT(name, ' ')[0] AS first_name, SPLIT(name, ' ')[1] as surname
FROM f1_processed.drivers

-- COMMAND ----------

SELECT current_timestamp() FROM f1_processed.drivers;

-- COMMAND ----------

SELECT date_format(dob, 'dd-MM-yyyy')
FROM f1_processed.drivers

-- COMMAND ----------

SELECT extract(MONTH FROM dob),
    CASE EXTRACT(MONTH FROM dob)
        WHEN 1 THEN 'January'
        WHEN 2 THEN 'February'
        WHEN 3 THEN 'March'
        WHEN 4 THEN 'April'
        WHEN 5 THEN 'May'
        WHEN 6 THEN 'June'
        WHEN 7 THEN 'July'
        WHEN 8 THEN 'August'
        WHEN 9 THEN 'September'
        WHEN 10 THEN 'October'
        WHEN 11 THEN 'November'
        WHEN 12 THEN 'December'
    END AS birth_month
FROM f1_processed.drivers

-- COMMAND ----------

SELECT  * 
FROM f1_processed.drivers
WHERE dob in (SELECT MAX(dob) FROM f1_processed.drivers)

-- COMMAND ----------

SELECT  nationality, name, dob, RANK() OVER(PARTITION BY nationality ORDER BY dob) as age_rank
FROM f1_processed.drivers
ORDER BY nationality, dob
