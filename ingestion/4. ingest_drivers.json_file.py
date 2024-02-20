# Databricks notebook source
# MAGIC %md
# MAGIC ##ingest drivers file
# MAGIC

# COMMAND ----------

#widget to add data source
dbutils.widgets.text('p_data_source','')
p_data_source = dbutils.widgets.get("p_data_source")

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

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------

driver_name_schema = StructType(fields=[StructField('forename', StringType(),True),
                                        StructField('surname', StringType(), True)])

# COMMAND ----------

drivers_schema = StructType(fields=[StructField('driverId', StringType(), False),
                                        StructField('driverRef', StringType(), True)
                                        , StructField('number', IntegerType(), True)
                                        , StructField('code', StringType(), True)
                                        , StructField('name', driver_name_schema)
                                        , StructField('dob', DateType(), True)
                                        , StructField('nationality', StringType(), True)
                                        , StructField('url', StringType(), True)
                                        ])

# COMMAND ----------

drivers_df = spark.read\
    .option('header', True)\
    .schema(drivers_schema)\
    .json(f'{raw_folder_path}/drivers.json')

# COMMAND ----------

# MAGIC %md
# MAGIC ###select rename columns and add ingestion date and data source columns

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

drivers_with_columns_df = ingestion_date(drivers_df).\
  withColumnRenamed('driverId', 'driver_id').\
  withColumnRenamed('driverRef', 'driver_ref').\
  withColumn('data_source', lit(p_data_source))



# COMMAND ----------

# MAGIC %md
# MAGIC ### create new column by concating forename and surname
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import concat, col

# COMMAND ----------

drivers_semi_final_df = drivers_with_columns_df.\
    withColumn("name", concat(col('name.forename'), lit(' '), col('name.surname')))

# COMMAND ----------

# MAGIC %md
# MAGIC ### drop url column

# COMMAND ----------

drivers_final_df = drivers_semi_final_df.drop('url')

# COMMAND ----------

# MAGIC %md
# MAGIC ### write the final dataframe to parquet file format
# MAGIC

# COMMAND ----------

drivers_final_df.write.mode('overwrite').parquet(f'{processed_folder_path}/drivers')
