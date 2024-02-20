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

races_df = spark.read\
    .schema(races_schema)\
    .csv(f'{raw_folder_path}/races.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ###select rename columns and add ingestion date and data source columns

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

races_renamed_df = ingestion_date(races_df).\
  withColumnRenamed('raceId', 'race_id').\
  withColumnRenamed('year', 'race_year').\
  withColumn('data_source', lit(p_data_source))



# COMMAND ----------

# MAGIC %md
# MAGIC ### drop url column

# COMMAND ----------

races_final_df = races_renamed_df.drop('url')

# COMMAND ----------

# MAGIC %md
# MAGIC ### write the final dataframe to parquet file format
# MAGIC

# COMMAND ----------

races_final_df.write.mode('overwrite').partitionBy('race_year').parquet(f'{processed_folder_path}/races')
