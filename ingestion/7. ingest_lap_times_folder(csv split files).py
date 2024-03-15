# Databricks notebook source
# MAGIC %md
# MAGIC ##ingest lap_times files
# MAGIC

# COMMAND ----------

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

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# COMMAND ----------

lap_times_schema = StructType(fields=[StructField('raceId', IntegerType(), False)
                                      , StructField('driverId', IntegerType(),True)
                                      , StructField('lap', IntegerType(),True)
                                      , StructField('position', IntegerType(), True)  
                                      , StructField('time', StringType(), True)
                                      , StructField('milliseconds', IntegerType(), True)
                                    ])

# COMMAND ----------

lap_times_df = spark.read\
    .schema(lap_times_schema)\
    .csv(f"{raw_folder_path}/{v_file_date}/lap_times")

# COMMAND ----------

# MAGIC %md
# MAGIC ### add ingestion date column and rename columns

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

lap_times_final_df = ingestion_date(lap_times_df).\
    withColumnRenamed('raceid', 'race_id').\
    withColumnRenamed('driverid', 'driver_id').\
    withColumn('data_source', lit(p_data_source)).\
    withColumn('file_date', lit(v_file_date))
    


# COMMAND ----------

# MAGIC %md
# MAGIC ### write the final dataframe to parquet file format
# MAGIC

# COMMAND ----------


merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.lap = src.lap"

# COMMAND ----------


merge_delta_df(lap_times_final_df, "f1_processed", "lap_times", processed_folder_path, merge_condition, "race_id")


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT race_id, count(1)
# MAGIC FROM f1_processed.lap_times
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC

# COMMAND ----------

dbutils.notebook.exit("Success")
