# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

circuits_df  = spark.read.parquet(f"{processed_folder_path}/circuits")\
    .withColumnRenamed('location', 'circuit_location')

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")\
    .withColumnRenamed('name', 'race_name')\
    .withColumnRenamed('race_timestamp', 'race_date')

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

constructors_df = spark.read.parquet(f"{processed_folder_path}/constructors")\
    .withColumnRenamed('name', 'team')

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

results_df = spark.read.parquet(f"{processed_folder_path}/results")\
    .withColumnRenamed('time', 'race_time')

# COMMAND ----------

display(results_df)

# COMMAND ----------

races_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner")\
    .select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location)

# COMMAND ----------

race_results_df = results_df.join(races_circuits_df, results_df.race_id == races_circuits_df.race_id)\
    .join(drivers_df, results_df.driver_id == drivers_df.driver_id)\
    .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id)

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

final_df = race_results_df.select(col('race_year'), col('race_name'), col('race_date'), col('circuit_location'),col('driver_name'),col('driver_number'), col('driver_nationality'), col('team'), col('grid'), col('fastest_lap'), col('race_time'), col('points'))\
.withColumn('created_date', current_timestamp())

# COMMAND ----------

display(final_df)

# COMMAND ----------

display(final_df.filter((final_df.race_year == 2020) & (final_df.race_name == 'Abu Dhabi Grand Prix')).orderBy(final_df.points.desc()))


# COMMAND ----------

final_df.write.mode('overwrite').parquet(f"{presentation_folder_path}/race_results")
