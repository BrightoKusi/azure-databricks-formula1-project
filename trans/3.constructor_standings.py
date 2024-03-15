# Databricks notebook source
# MAGIC %md
# MAGIC ###produce constructor standings

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
.filter(f"file_date = '{v_file_date}'") 

# COMMAND ----------

display(race_results_df.filter('race_year =  2020'))

# COMMAND ----------

from pyspark.sql.functions import sum, when, col, count

constructors_standings_df = race_results_df\
    .groupBy('race_year', 'team')\
    .agg(sum('points').alias('total_points'),
         count(when(col('position') == 1, True )).alias('wins'))

# COMMAND ----------

display(constructors_standings_df.filter('race_year = 2020'))

# COMMAND ----------

#rank drivers based on number of wins

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank
constructors_rank_spec = Window.partitionBy('race_year').orderBy(desc('total_points'), desc('wins'))
final_df = constructors_standings_df.withColumn('rank', rank().over(constructors_rank_spec))

# COMMAND ----------

display(final_df.filter('race_year = 2020'))

# COMMAND ----------


final_df.write.mode('overwrite').format('parquet').saveAsTable('f1_presentation.constructor_standings')

