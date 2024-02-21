# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

circuits_df  = spark.read.parquet(f"{processed_folder_path}/circuits")\
    .withColumnRenamed('location', 'circuit_location')

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races").filter("race_year=2019")\
    .withColumnRenamed('date', 'race_date')\
    .withColumnRenamed('name', 'race_name')

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

display(races_df)

# COMMAND ----------

drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers")\
    .withColumnRenamed('name', 'driver_name')\
    .withColumnRenamed('number', 'driver_number')\
    .withColumnRenamed('nationality', 'driver_nationality')
                                

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

constructors_df = spark.read.parquet(f"{processed_folder_path}/constructors")

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

results_df = spark.read.parquet(f"{processed_folder_path}/results")\
    .withColumnRenamed('time', 'race_time')

# COMMAND ----------

display(results_df)

# COMMAND ----------

races_circuits_df = races_df.join(circuits_df, races_df.circuitId == circuits_df.circuit_id, "inner")\
    .select(races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location)

# COMMAND ----------


