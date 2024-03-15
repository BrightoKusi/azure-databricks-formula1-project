# Databricks notebook source
# MAGIC %md
# MAGIC ###produce driver standings

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ###Find race years for which the data is to be reprocessed

# COMMAND ----------


race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
.filter(f"file_date = '{v_file_date}'")\
.select("race_year")\
.distinct()\
.collect()

# COMMAND ----------

race_year_list = []
for race_year in race_year_list:
    race_year_list.append(race_year.race_year)

print(race_year_list)

# COMMAND ----------

race_year_list = df_column_to_list(race_results_df, 'race_year')

# COMMAND ----------

from pyspark.sql.functions import sum, when, col, count

driver_standings_df = race_results_df\
    .groupBy('race_year', 'driver_name', 'driver_nationality', 'team')\
    .agg(sum('points').alias('total_points'),
         count(when(col('position') == 1, True )).alias('wins'))

# COMMAND ----------

display(driver_standings_df)

# COMMAND ----------

#rank drivers based on number of wins

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank
driver_rank_spec = Window.partitionBy('race_year').orderBy(desc('total_points'), desc('wins'))
final_df = driver_standings_df.withColumn('rank', rank().over(driver_rank_spec))

# COMMAND ----------

display(final_df.filter('race_year = 2020'))

# COMMAND ----------


final_df.write.mode('overwrite').format('parquet').saveAsTable('f1_presentation.driver_standings')

