# Databricks notebook source
# MAGIC %md
# MAGIC ##ingest races.csv file
# MAGIC

# COMMAND ----------

dbutils.widgets.text('p_data_source','')
p_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ### specify schema 

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------

races_schema = StructType(fields=[StructField('raceId', IntegerType(), False)
                                      , StructField('year', IntegerType(),True)
                                      , StructField('round', IntegerType(), True)  
                                      , StructField('circuitId', IntegerType(), True)
                                      , StructField('name', StringType(), True)
                                      , StructField('date', DateType(), True)
                                      , StructField('time', StringType(), True)
                                      , StructField('url', StringType(), True)
                                      ])

# COMMAND ----------

races_df = spark.read \
.option("header", True) \
.schema(races_schema) \
.csv(f"{raw_folder_path}/races.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ###add ingestion date and concat date and time columns and convert to timestamp

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, lit, col, concat

# COMMAND ----------

races_modified_df = ingestion_date(races_df)\
    .withColumn('race_timestamp', to_timestamp(concat(col('date'), lit(' '), col('time')),'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

# MAGIC %md 
# MAGIC ###rename the columns

# COMMAND ----------

races_renamed_df = races_modified_df.\
  withColumnRenamed('raceId', 'race_id').\
  withColumnRenamed('year', 'race_year').\
  withColumnRenamed('circuitId', 'circuit_id').\
  withColumn('data_source', lit(p_data_source))



# COMMAND ----------

# MAGIC %md
# MAGIC ### drop url column

# COMMAND ----------

races_dropped_df = races_renamed_df.drop('url')

# COMMAND ----------

# MAGIC %md
# MAGIC ### select the required columns
# MAGIC

# COMMAND ----------

races_final_df = races_dropped_df.select(col('race_id'), col('race_year'), col('round'), col('circuit_id'), col('name'), col('ingestion_date'), col('race_timestamp'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### write the final dataframe to parquet file format
# MAGIC

# COMMAND ----------

races_final_df.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.races')

# COMMAND ----------

dbutils.notebook.exit("Success")
