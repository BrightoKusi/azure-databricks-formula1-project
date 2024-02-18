# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake using entrant id
# MAGIC 1. Register Azure entrant id app
# MAGIC 2. Generate a secret for the app
# MAGIC 3. Set spark config with with client-id, tenant-id and Secret
# MAGIC 4. Assign role 'Storage blob data contributor' to the data lake

# COMMAND ----------

client_id = dbutils.secrets.get(scope='formula1-scope', key='formula1-app-client-id')
tenant_id = dbutils.secrets.get(scope='formula1-scope', key='formula1-app-tenant-id')
client_secret = dbutils.secrets.get(scope='formula1-scope', key='formula1-app-client-secret')


# COMMAND ----------


spark.conf.set("fs.azure.account.auth.type.formula1bk.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1bk.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1bk.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1bk.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1bk.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}


# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1bk.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1bk.dfs.core.windows.net"))
