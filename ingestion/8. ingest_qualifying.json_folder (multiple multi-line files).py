# Databricks notebook source
# MAGIC %md
# MAGIC ##ingest qualifying files
# MAGIC

# COMMAND ----------

#widget to add data source
dbutils.widgets.text('p_data_source','')
p_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text('v_file_date','2021-03-21')
v_file_date = dbutils.widgets.get("v_file_date")


# COMMAND ----------

# MAGIC
# MAGIC
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ### specify schema 

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

qualifying_schema = StructType(fields=[StructField('qualifyId', IntegerType(), False)
                                        , StructField('raceId', IntegerType(), True)
                                        , StructField('driverId', IntegerType(), True)
                                        , StructField('constructorId', IntegerType(), True)
                                        , StructField('number', IntegerType(), True)
                                        , StructField('position', IntegerType(), True)
                                        , StructField('q1', StringType(), True)
                                        , StructField('q2', StringType(), True)
                                        , StructField('q3', StringType(), True)
                                        ])

# COMMAND ----------

qualifying_df = spark.read\
    .schema(qualifying_schema)\
    .option('multiLine', True)\
    .json(f'{raw_folder_path}/{v_file_date}/qualifying')

# COMMAND ----------

# MAGIC %md
# MAGIC ###select rename columns and add ingestion date and data source columns

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

qualifying_final_df = ingestion_date(qualifying_df).\
  withColumnRenamed('qualifyId', 'qualify_id').\
  withColumnRenamed('raceId', 'race_id').\
  withColumnRenamed('constructorId', 'constructor_id').\
  withColumnRenamed('driverId', 'driver_id').\
  withColumn('data_source', lit(p_data_source)).\
  withColumn('file_date', lit(v_file_date))



# COMMAND ----------

# MAGIC %md
# MAGIC ### write the final dataframe to parquet file format
# MAGIC

# COMMAND ----------

merge_condition = "tgt.qualify_id = src.qualify_id AND tgt.race_id = src.race_id"


# COMMAND ----------


merge_delta_df(qualifying_final_df, "f1_processed", "qualifying", processed_folder_path, merge_condition, "race_id")


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT race_id, count(1)
# MAGIC FROM f1_processed.qualifying
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC

# COMMAND ----------

dbutils.notebook.exit("Success")
