# Databricks notebook source
# MAGIC %md
# MAGIC ###create a function to add new column for ingestion date

# COMMAND ----------


def ingestion_date(input_df):
  from pyspark.sql.functions import current_timestamp
  output_df = input_df.withColumn("ingestion_date", current_timestamp())
  return output_df
