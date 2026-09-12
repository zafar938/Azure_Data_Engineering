# Databricks notebook source
# MAGIC %run ./ADLS_oauth
# MAGIC

# COMMAND ----------

df = (spark.read.format("csv")
      .option("path","abfss://oanoperationsdev@90111adlsdevz.dfs.core.windows.net/oaon.sandbox.operations.dynamics.com/Tables/Purchase/Parties/")
      .load()
      )
display(df)

# COMMAND ----------

service_credential = dbutils.secrets.get(scope="adbdevscope",key="ClientSecret")
appid = dbutils.secrets.get(scope="adbdevscope",key="appid")
tenantid = dbutils.secrets.get(scope="adbdevscope",key="tenantid")

# COMMAND ----------

df = (spark.read.format("com.microsoft.cdm")  # Ensure the correct package/connector is installed and available in the classpath. Make sure to include the necessary package in the cluster configuration, e.g., via spark.jars.packages, e.g., 'com.microsoft.azure:azure-cosmos-spark_3-2_2-12:4.0.0'.
  .option("storage", "90111adlsdevz.dfs.core.windows.net")
   .option("appid",appid)
  .option("appkey",service_credential) 
 .option("tenantid",tenantid)
  .option("manifestPath", "oanoperationsdev/oaon-sandbox.operations.dynamics.com/Tables/Purchase/Purchase.manifest.cdm.json")
  .option("entity", "Parties")
#  .option("mode", "permissive")
  .load())
display(df)
