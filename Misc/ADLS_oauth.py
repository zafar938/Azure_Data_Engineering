# Databricks notebook source
service_credential = dbutils.secrets.get(scope="adbdevscope",key="ClientSecret")
appid = dbutils.secrets.get(scope="adbdevscope",key="appid")
tenantid = dbutils.secrets.get(scope="adbdevscope",key="tenantid")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.90111adlsdevz.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.90111adlsdevz.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.90111adlsdevz.dfs.core.windows.net", appid)
spark.conf.set("fs.azure.account.oauth2.client.secret.90111adlsdevz.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.90111adlsdevz.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenantid}/oauth2/token")