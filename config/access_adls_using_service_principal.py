# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using Service Principal
# MAGIC 1. Register Azure AD Application/Service Principal
# MAGIC 1. Generate a secret/password for the Application
# MAGIC 1. Set Spark Config with App/Client ID, Diretory/Tenant ID & Secret
# MAGIC 1. Assign Role 'Storage Blob Data Contributor' to the Data Lake

# COMMAND ----------

client_id = dbutils.secrets.get(scope='scope-project-lake', key='secret-app-application-client-id')
tenant_id = dbutils.secrets.get(scope='scope-project-lake', key='secret-app-application-tenant-id')
client_secret = dbutils.secrets.get(scope='scope-project-lake', key='secret-app-application-secret-value')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.dlsprojlakehousedev.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.dlsprojlakehousedev.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.dlsprojlakehousedev.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.dlsprojlakehousedev.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.dlsprojlakehousedev.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")
