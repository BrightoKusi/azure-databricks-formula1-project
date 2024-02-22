# Databricks notebook source

def ingestion_date(input_df):
  from pyspark.sql.functions import current_timestamp
  output_df = input_df.withColumn("ingestion_date", current_timestamp())
  return output_df
