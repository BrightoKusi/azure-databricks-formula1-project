# Databricks notebook source
# MAGIC %md
# MAGIC ##ingest circuits.csv file
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

circuits_schema = StructType(fields=[StructField('circuitId', IntegerType(), False)
                                      , StructField('circuitRef', StringType(),True)
                                      , StructField('name', StringType(), True)  
                                      , StructField('location', StringType(), True)
                                      , StructField('country', StringType(), True)
                                      , StructField('lat', DoubleType(), True)
                                      , StructField('lng', DoubleType(), True)
                                      , StructField('alt', DoubleType(), True)
                                      , StructField('url', StringType(), True)
                                      ])

# COMMAND ----------

circuits_df = spark.read\
    .option('header', True)\
    .schema(circuits_schema)\
    .csv(f'{raw_folder_path}/circuits.csv')

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###select columns and rename

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_selected_df = circuits_df.select(col('circuitId').alias('circuit_id'),\
  col('circuitRef').alias('circuit_ref'), \
  col('name'), \
  col('location'), \
  col('country'), \
  col('lat').alias('latitude'), \
  col('lng').alias('longitude'), \
  col('alt').alias('altitude'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### add columns to show ingestion date and data source

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_final_df = ingestion_date(circuits_selected_df).\
    withColumn('data_source', lit(p_data_source))


# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### write the final dataframe to parquet file format
# MAGIC

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("parquet").saveAsTable('f1_processed.circuits')

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/circuits"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT* FROM f1_processed.circuits

# COMMAND ----------

dbutils.notebook.exit("Success")
