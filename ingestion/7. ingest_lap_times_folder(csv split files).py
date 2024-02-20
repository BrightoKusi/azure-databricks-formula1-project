# Databricks notebook source
# MAGIC %md
# MAGIC ##ingest lap_times files
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
    .csv(f'{raw_folder_path}/lap_times')

# COMMAND ----------

# MAGIC %md
# MAGIC ### add ingestion date column and rename columns

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

lap_times_final_df = ingestion_date(lap_times_df).\
    withColumnRenamed('raceid', 'race_id').\
    withColumnRenamed('driverid', 'driver_id').\
    withColumn('data_source', lit(p_data_source))


# COMMAND ----------

# MAGIC %md
# MAGIC ### write the final dataframe to parquet file format
# MAGIC

# COMMAND ----------

lap_times_final_df.write.mode('overwrite').parquet(f'{processed_folder_path}/lap_times')
