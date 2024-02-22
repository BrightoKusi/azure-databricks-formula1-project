# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f'{presentation_folder_path}/race_results')

# COMMAND ----------

demo_df = race_results_df.filter('race_year = 2020')

# COMMAND ----------

display(demo_df)

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum

# COMMAND ----------

#count all races for the year
display(demo_df.select(count('*')))

# COMMAND ----------

#count all distinct races for the year 2020
display(demo_df.select(countDistinct('race_name')))

# COMMAND ----------

#sum of all race points
display(demo_df. select(sum('points')))

# COMMAND ----------

#sum of all race points
display(demo_df.filter(demo_df.driver_name == 'Lewis Hamilton').\
    select(sum('points'), countDistinct('race_name')).\
    withColumnRenamed('sum(points)', 'total_points').\
    withColumnRenamed( 'count(DISTINCT race_name)', 'number_of_races'))

# COMMAND ----------

display(demo_df\
    .groupBy('driver_name')\
    .sum('points'))

# COMMAND ----------

display(demo_df\
    .groupBy('driver_name')\
    .agg(sum('points').alias('sum_of_points'), countDistinct('race_name').alias('number_of_races')))

# COMMAND ----------

# MAGIC %md
# MAGIC ###window functions

# COMMAND ----------

demo_df = race_results_df.filter('race_year IN (2019, 2020)')

# COMMAND ----------

display(demo_df)

# COMMAND ----------

demo_grouped_df = demo_df\
    .groupBy('race_year', 'driver_name')\
    .agg(sum('points').alias('total_points'), countDistinct('race_name').alias('number_of_races'))

# COMMAND ----------

#rank drivers based on number of points

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank
driverRankSpec = Window.partitionBy('race_year').orderBy('total_points')
display(demo_grouped_df.withColumn('rank', rank().over(driverRankSpec)))

# COMMAND ----------


