# Databricks notebook source
# MAGIC %md
# MAGIC ### Access dataframes using SQL
# MAGIC 1. Create temporary view on dataframes
# MAGIC 2. Access the view from SQL cell
# MAGIC 3. Access the view from Python cell

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f'{presentation_folder_path}/race_results')

# COMMAND ----------

race_results_df.createOrReplaceTempView('v_race_results')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM v_race_results
# MAGIC WHERE race_year = 2020

# COMMAND ----------

race_results_2019_df = spark.sql('SELECT * FROM v_race_results WHERE race_year = 2020')

# COMMAND ----------

display(race_results_2019_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Global Temporary Views
# MAGIC 1. Create global temporary views on dataframes
# MAGIC 2. Access the view from SQL cell
# MAGIC 3. Access the view from Python cell
# MAGIC 4. Access the view from another notebooks

# COMMAND ----------

race_results_df.createOrReplaceTempView('gv_race_results')

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gv_race_results

# COMMAND ----------

race_results_2018_df =  spark.sql('SELECT * FROM gv_race_results WHERE race_year = 2018 ')
display(race_results_2018_df)
