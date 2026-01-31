# Databricks notebook source
# MAGIC %md
# MAGIC **set adls access via Service Principal Name,key vault and secret scope in databricks**

# COMMAND ----------


client_id  = dbutils.secrets.get(scope="learning-scope", key="f1learningdl-client-id")
tenant_id  = dbutils.secrets.get(scope="learning-scope", key="f1learningdl-tennat-id")
secret     = dbutils.secrets.get(scope="learning-scope", key="fllearningdl-secret-id")

spark.conf.set("fs.azure.account.auth.type.f1learningdl.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.f1learningdl.dfs.core.windows.net",
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.f1learningdl.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.f1learningdl.dfs.core.windows.net", secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.f1learningdl.dfs.core.windows.net",
               f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")


# COMMAND ----------

#display(dbutils.fs.ls("abfss://raw@f1learningdl.dfs.core.windows.net/raw"))

# COMMAND ----------

#df=spark.read.csv("abfss://raw@f1learningdl.dfs.core.windows.net/raw/circuits.csv")
