# Databricks notebook source
dbutils.notebook.help()

# COMMAND ----------

v_result = dbutils.notebook.run('1. ingest_circuits.csv_file', 0, {'p_data_source':'Ergast API'})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run('2. ingest_races.csv_file', 0, {'p_data_source':'Ergast API'})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run('3. ingest_constructors.json_file', 0, {'p_data_source':'Ergast API'})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run('4. ingest_drivers.json_file', 0, {'p_data_source':'Ergast API'})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run('5. ingest_results.json_file', 0, {'p_data_source':'Ergast API'})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run('6. ingest_pit_stops.json_file', 0, {'p_data_source':'Ergast API'})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run('7. ingest_lap_times_folder(csv split files)', 0, {'p_data_source':'Ergast API'})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run('8. ingest_qualifying.json_folder (multiple multi-line files)', 0, {'p_data_source':'Ergast API'})

# COMMAND ----------

v_result
