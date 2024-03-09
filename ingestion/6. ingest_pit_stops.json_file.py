# Databricks notebook source
# MAGIC %md
# MAGIC ##ingest pit_stops file
# MAGIC

# COMMAND ----------

#widget to add data source
dbutils.widgets.text('p_data_source','')
p_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

#widget to add data source
dbutils.widgets.text('v_file_date','2021-03-21')
v_file_date = dbutils.widgets.get("v_file_date")

# COMMAND ----------

# MAGIC
# MAGIC
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC
# MAGIC
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ### specify schema 

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

pit_stops_schema = StructType(fields=[StructField('raceId', IntegerType(), False),
                                        StructField('driverId', IntegerType(), True)
                                        , StructField('stop', IntegerType(), True)
                                        , StructField('lap', IntegerType(), True)
                                        , StructField('time', StringType(), True)
                                        , StructField('duration', StringType(), True)
                                        , StructField('milliseconds', IntegerType(), True)
                                        ])

# COMMAND ----------

pit_stops_df = spark.read\
    .schema(pit_stops_schema)\
    .option('multiLine', True)\
    .json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ###select rename columns and add ingestion date and data source columns

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

pit_stops_final_df = ingestion_date(pit_stops_df).\
  withColumnRenamed('raceId', 'race_id').\
  withColumnRenamed('driverId', 'driver_id').\
  withColumn('data_source', lit(p_data_source)).\
  withColumn('file_date', lit(v_file_date))



# COMMAND ----------

display(pit_stops_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### write the final dataframe to parquet file format
# MAGIC

# COMMAND ----------


merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop"
merge_delta_df(pit_stops_final_df, "f1_processed", "pit_stops", processed_folder_path, merge_condition, "race_id")


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT race_id, count(1)
# MAGIC FROM f1_processed.pit_stops
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC
# MAGIC

# COMMAND ----------

dbutils.notebook.exit("Success")
