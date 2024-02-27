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
# MAGIC ### write the final dataframe to parquet file format
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Incremental load - Method 1

# COMMAND ----------

# #Drop partitions if exist and write them again

# for race_id_list in results_final_df.select("race_id").distinct().collect():
#     if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#         spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")


# COMMAND ----------

results_final_df.write.mode('append').partitionBy('race_id').format("parquet").saveAsTable('f1_processed.results')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT race_id, count(1)
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE IF EXISTS f1_processed.results;

# COMMAND ----------

# spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# COMMAND ----------

# def rearrange_partition_column(input_df, partition_column):
#     column_list = []
#     for column_name in input_df.schema.names:
#         if column_name != partition_column:
#             column_list.append(column_name)
#     column_list.append(partition_column)

#     print(column_list)

#     output_df = results_final_df.select(column_list)
#     return output_df

# COMMAND ----------

# output_df = rearrange_partition_column(results_final_df, 'race_id')

# COMMAND ----------

# results_final_df = results_final_df.select("result_id", "driver_id", "constructor_id", "number", "grid", "position", "position_text", "position_order", "points", "laps", "time", "milliseconds", "fastest_lap", "rank", "fastest_lap_time", "fastest_lap_speed", "data_source", "file_date", "ingestion_date", "race_id")

# COMMAND ----------

# def overwrite_partition(input_df, db_name, table_name, partition_column):
#     output_df = rearrange_partition_column(input_df, partition_column)
    
#     spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
  
#     if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
#         output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
#     else:
#         output_df.write.mode("overwrite").partitionBy(partition_column).format("parquet").saveAsTable(f"{db_name}.{table_name}")



# COMMAND ----------

# overwrite_partition(results_final_df, "f1_processed", "results", "race_id" )

# COMMAND ----------

# if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#     results_final_df.write.mode("overwrite").insertInto("f1_processed.results")
# else:
#     results_final_df.write.mode('overwrite').partitionBy('race_id').format("parquet").saveAsTable('f1_processed.results')

# COMMAND ----------

# %sql
# SELECT COUNT(*) FROM f1_processed.results

# COMMAND ----------

dbutils.notebook.exit("Success")
