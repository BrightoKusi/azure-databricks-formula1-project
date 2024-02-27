# Databricks notebook source
# MAGIC %md
# MAGIC #Delta Lake
# MAGIC 1. Write data to lake (mananged and external tables)
# MAGIC 2. Read data from lake (Table and file)

# COMMAND ----------

# MAGIC %md
# MAGIC ## create a container and mount to data bricks

# COMMAND ----------

def mount_adls(storage_account_name, container_name):
    # Get secrets from secrets vault
    client_id = dbutils.secrets.get(scope='formula1-scope', key='formula1-app-client-id')
    tenant_id = dbutils.secrets.get(scope='formula1-scope', key='formula1-app-tenant-id')
    client_secret = dbutils.secrets.get(scope='formula1-scope', key='formula1-app-client-secret')

    # Set spark configuration
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

    # Unmount the mount point if it already exists
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")

    # Mount the storage account container
    dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs)

    display(dbutils.fs.mounts())
        

# COMMAND ----------

mount_adls('formula1bk', 'delta')

# COMMAND ----------

# MAGIC %md
# MAGIC ### create a database

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE  IF NOT EXISTS f1_delta
# MAGIC LOCATION "/mnt/formula1bk/delta";

# COMMAND ----------

results_df = spark.read\
    .option("inferschema", True)\
    .json("/mnt/formula1bk/raw/2021-03-28/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ### create a managed table
# MAGIC

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").saveAsTable("f1_delta.results_managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_delta.results_managed

# COMMAND ----------

# MAGIC %md 
# MAGIC ### create an external table
# MAGIC

# COMMAND ----------

results_df.write.format("delta").mode("append").save("/mnt/formula1bk/delta/results_external")

# COMMAND ----------

results_external_df = spark.read.format("delta").load("/mnt/formula1bk/delta/results_external")

# COMMAND ----------

display(results_external_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### write to a partitioned delta lake

# COMMAND ----------

results_df.write.format("delta").partitionBy('constructorId').mode("overwrite").saveAsTable("f1_delta.results_partition")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Update table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_delta.results_managed

# COMMAND ----------

# MAGIC %md
# MAGIC ###using sql

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE f1_delta.results_managed 
# MAGIC SET points = 11 - position 
# MAGIC WHERE position <= 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_delta.results_managed

# COMMAND ----------

# MAGIC %md
# MAGIC ###using python

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/mnt/formula1bk/delta/results_managed")

# Declare the predicate by using a SQL-formatted string.
deltaTable.update(
  condition = "position <= 10",
  set = { "points": "21- position" }
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_delta.results_managed

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_delta.results_managed WHERE position > 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_delta.results_managed

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/mnt/formula1bk/delta/results_managed")

# Declare the predicate by using a SQL-formatted string.
deltaTable.delete(
  condition = "points = 0",
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_delta.results_managed

# COMMAND ----------

# MAGIC %md
# MAGIC ### Upsert using Merge:
# MAGIC 1. Merge allows to insert, update and delete in one statement
# MAGIC  

# COMMAND ----------

drivers_day1_df = spark.read\
    .option("inferSchema", True)\
    .json("/mnt/formula1bk/raw/2021-03-28/drivers.json")\
    .filter("driverId <= 10")\
    .select("driverId", "dob", "name.forename", "name.surname")

# COMMAND ----------

display(drivers_day1_df)

# COMMAND ----------

drivers_day1_df.createOrReplaceTempView("drivers_day1")

# COMMAND ----------

from pyspark.sql.functions import upper
drivers_day2_df = spark.read\
    .option("inferSchema", True)\
    .json("/mnt/formula1bk/raw/2021-03-28/drivers.json")\
    .filter("driverId BETWEEN 6 and 15")\
    .select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

drivers_day2_df.createOrReplaceTempView("drivers_day2")

# COMMAND ----------

display(drivers_day2_df)

# COMMAND ----------

from pyspark.sql.functions import upper
drivers_day3_df = spark.read\
    .option("inferSchema", True)\
    .json("/mnt/formula1bk/raw/2021-03-28/drivers.json")\
    .filter("driverId BETWEEN 1 and 5 OR driverId BETWEEN 16 and 20")\
    .select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

display(drivers_day3_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_delta.drivers_merge;
# MAGIC CREATE TABLE IF NOT EXISTS f1_delta.drivers_merge(
# MAGIC   driverId INT
# MAGIC   , dob DATE
# MAGIC   , forename STRING
# MAGIC   , surname STRING
# MAGIC   , createdDate DATE
# MAGIC   , updatedDate DATE 
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_delta.drivers_merge tgt
# MAGIC USING drivers_day1 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     tgt.dob = upd.dob
# MAGIC     , tgt.forename = upd.forename
# MAGIC     , tgt.surname = upd.surname
# MAGIC     , tgt.updatedDate = current_timestamp
# MAGIC    
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (
# MAGIC    driverId, dob, forename, surname, createdDate
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     upd.driverId
# MAGIC     , upd.dob
# MAGIC     , upd.forename
# MAGIC     , upd.surname
# MAGIC     , current_timestamp
# MAGIC   )

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM f1_delta.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_delta.drivers_merge tgt
# MAGIC USING drivers_day2 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     tgt.dob = upd.dob
# MAGIC     , tgt.forename = upd.forename
# MAGIC     , tgt.surname = upd.surname
# MAGIC     , tgt.updatedDate = current_timestamp
# MAGIC    
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (
# MAGIC    driverId, dob, forename, surname, createdDate
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     upd.driverId
# MAGIC     , upd.dob
# MAGIC     , upd.forename
# MAGIC     , upd.surname
# MAGIC     , current_timestamp
# MAGIC   )

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM f1_delta.drivers_merge

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/mnt/formula1bk/delta/drivers_merge")

deltaTable.alias('tgt') \
  .merge(
    drivers_day3_df.alias('upd'),
    "tgt.driverId = upd.driverId"
  ) \
  .whenMatchedUpdate(set =
    {
      "dob": "upd.dob",
      "forename": "upd.forename",
      "surname": "upd.surname",
      "updatedDate":"current_timestamp()"
    }
  ) \
  .whenNotMatchedInsert(values =
    {
      "driverId": "upd.driverId",
      "dob":"upd.dob",
      "forename": "upd.forename",
      "surname": "upd.surname",
      "createdDate": "current_timestamp()"
    }
  ) \
  .execute() 

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM f1_delta.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC ### Advantages of Delta Lake tables
# MAGIC 1. History & Versioning
# MAGIC 2. Time travel
# MAGIC 3. Vacuum

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_delta.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Querying previous versions
# MAGIC
# MAGIC SELECT * FROM f1_delta.drivers_merge VERSION AS OF 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Querying previous versions 
# MAGIC -- Or using the timestamp
# MAGIC
# MAGIC SELECT * FROM f1_delta.drivers_merge TIMESTAMP AS OF "2024-02-26T13:38:47Z";
# MAGIC

# COMMAND ----------

version2_df = spark.read.format("delta").option("timestampAsOf", "2024-02-26T13:38:47Z").load("/mnt/formula1bk/delta/drivers_merge")

display(version2_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Using Vacuum command to completely remove a record even from previous versions
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC VACUUM f1_delta.drivers_merge RETAIN 0 HOURS

# COMMAND ----------

version2_df = spark.read.format("delta").option("timestampAsOf", "2024-02-26T13:38:47Z").load("/mnt/formula1bk/delta/drivers_merge")

display(version2_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Time travel to restore deleted or modified data

# COMMAND ----------

# MAGIC %sql
# MAGIC -- delete data for demo
# MAGIC DELETE 
# MAGIC FROM f1_delta.drivers_merge
# MAGIC WHERE driverId = 1
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Revert to an earlier version of the data using the versioning and time travel
# MAGIC MERGE INTO f1_delta.drivers_merge tgt
# MAGIC USING f1_delta.drivers_merge VERSION AS OF 3 src
# MAGIC   ON (tgt.driverId = src.driverId)
# MAGIC WHEN NOT MATCHED THEN  
# MAGIC   INSERT * 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM f1_delta.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC ### convert from parquet to delta

# COMMAND ----------

# MAGIC %sql
# MAGIC -- create a parquet table
# MAGIC CREATE TABLE IF NOT EXISTS f1_delta.drivers_convert_to_delta(
# MAGIC   driverId INT
# MAGIC   , dob DATE
# MAGIC   , forename STRING
# MAGIC   , surname STRING
# MAGIC   , createdDate DATE
# MAGIC   , updatedDate DATE
# MAGIC )
# MAGIC USING PARQUET
