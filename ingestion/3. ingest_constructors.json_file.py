# Databricks notebook source
# MAGIC %md
# MAGIC ##ingest constructors file
# MAGIC

# COMMAND ----------

#widget to add data source
dbutils.widgets.text('p_data_source','')
p_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ### specify schema 

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_df = spark.read\
    .option('header', True)\
    .schema(constructors_schema)\
    .json(f'{raw_folder_path}/constructors.json')

# COMMAND ----------

# MAGIC %md
# MAGIC ###select rename columns and add ingestion date and data source columns

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

constructors_with_column_df = ingestion_date(constructors_df).\
  withColumnRenamed('constructorId', 'constructors_id').\
  withColumnRenamed('constructorRef', 'constructor_ref').\
  withColumn('data_source', lit(p_data_source))



# COMMAND ----------

# MAGIC %md
# MAGIC ### drop url column

# COMMAND ----------

constructors_final_df = constructors_with_column_df.drop('url')

# COMMAND ----------

# MAGIC %md
# MAGIC ### write the final dataframe to parquet file format
# MAGIC

# COMMAND ----------

constructors_final_df.write.mode('overwrite').parquet(f'{processed_folder_path}/constructors')
