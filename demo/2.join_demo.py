# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

circuits_df  = spark.read.parquet(f"{processed_folder_path}/circuits")\
    .withColumnRenamed('name', 'circuit_name')

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races").filter("race_year=2019")\
    .withColumnRenamed('name', 'race_name')

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

display(races_df)

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuitId, "inner")\
    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

display(race_circuits_df.select(circuits_df.circuit_name))
