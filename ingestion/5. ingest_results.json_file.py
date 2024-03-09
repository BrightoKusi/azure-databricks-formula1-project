# Databricks notebook source
# MAGIC %md
# MAGIC ##ingest results file
# MAGIC

# COMMAND ----------

#widget to add data source
dbutils.widgets.text('p_data_source','')
p_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text('v_file_date','2021-03-21')
v_file_date = dbutils.widgets.get("v_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ### specify schema 

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DoubleType

# COMMAND ----------

results_schema = StructType(fields=[StructField('resultId', IntegerType(), False)
                                        , StructField('raceId', IntegerType(), True)
                                        , StructField('driverId', IntegerType(), True)
                                        , StructField('constructorId', IntegerType(), True)
                                        , StructField('number', IntegerType(), True)
                                        , StructField('grid', IntegerType(), True)
                                        , StructField('position', IntegerType(), True)
                                        , StructField('positionText', StringType(), True)
                                        , StructField('positionOrder', IntegerType(), True)
                                        , StructField('points', DoubleType(), True)
                                        , StructField('laps', IntegerType(), True)
                                        , StructField('time', StringType(), True)
                                        , StructField('milliseconds', IntegerType(), True)
                                        , StructField('fastestLap', IntegerType(), True)
                                        , StructField('rank', IntegerType(), True)
                                        , StructField('fastestLapTime', StringType(), True)
                                        , StructField('fastestLapSpeed', DoubleType(), True)
                                        , StructField('statusId', IntegerType(), True)
                                        ])

# COMMAND ----------

results_df = spark.read\
    .schema(results_schema)\
    .json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ###select rename columns and add ingestion date and data source columns

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

results_columns_df = ingestion_date(results_df).\
  withColumnRenamed('resultId', 'result_id').\
  withColumnRenamed('raceId', 'race_id').\
  withColumnRenamed('constructorId', 'constructor_id').\
  withColumnRenamed('driverId', 'driver_id').\
  withColumnRenamed('positionText', 'position_text').\
  withColumnRenamed('positionOrder', 'position_order').\
  withColumnRenamed('fastestLap', 'fastest_lap').\
  withColumnRenamed('fastestLapTime', 'fastest_lap_time').\
  withColumnRenamed('fastestLapSpeed', 'fastest_lap_speed').\
  withColumn('data_source', lit(p_data_source)).\
  withColumn('file_date', lit(v_file_date))



# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop column

# COMMAND ----------

results_final_df = results_columns_df.drop('statusId')

# COMMAND ----------

# MAGIC %md
# MAGIC ### de-dupe the dataframe

# COMMAND ----------

results_deduped_df = results_final_df.dropDuplicates(["race_id", "driver_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### write the final dataframe to parquet file format
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Incremental load - Method 1

# COMMAND ----------

merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"

# COMMAND ----------


merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
merge_delta_df(results_deduped_df, "f1_processed", "results", processed_folder_path, merge_condition, "race_id")


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT race_id, count(1)
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC

# COMMAND ----------

dbutils.notebook.exit("Success")
