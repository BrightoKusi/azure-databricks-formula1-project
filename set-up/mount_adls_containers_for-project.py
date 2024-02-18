# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake using entrant id
# MAGIC 1. Register Azure entrant id app
# MAGIC 2. Generate a secret for the app
# MAGIC 3. Set spark config with with client-id, tenant-id and Secret
# MAGIC 4. Assign role 'Storage blob data contributor' to the data lake

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

mount_adls('formula1bk', 'raw')


# COMMAND ----------

mount_adls('formula1bk', 'processed')


# COMMAND ----------

mount_adls('formula1bk', 'presentation')


# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1bk/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/formula1bk/demo/circuits.csv"))
