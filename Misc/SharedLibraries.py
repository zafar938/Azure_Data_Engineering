# Databricks notebook source
import pyspark.sql.functions as F 
import datetime
import pandas as pd
import dateutil

# COMMAND ----------

ADLS_DEV_BASE_PATH = "abfss://oanoperationsdev@90111adlsdevz.dfs.core.windows.net/"
DELTALAKE_RAW_PATH = "DeltaLake/Raw/"

# COMMAND ----------

service_credential = dbutils.secrets.get(scope="adbdevscope",key="ClientSecret")
appid = dbutils.secrets.get(scope="adbdevscope",key="appid")
tenantid = dbutils.secrets.get(scope="adbdevscope",key="tenantid")


# COMMAND ----------

service_credential = dbutils.secrets.get(scope="adbdevscope",key="ClientSecret")
appid = dbutils.secrets.get(scope="adbdevscope",key="appid")
tenantid = dbutils.secrets.get(scope="adbdevscope",key="tenantid")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.90111adlsdevz.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.90111adlsdevz.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.90111adlsdevz.dfs.core.windows.net", appid)
spark.conf.set("fs.azure.account.oauth2.client.secret.90111adlsdevz.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.90111adlsdevz.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenantid}/oauth2/token")


# COMMAND ----------

def readfiles(folder_group, entity):
    csv_path = f"{ADLS_DEV_BASE_PATH}oaon.sandbox.operations.dynamics.com/Tables/{folder_group}/{entity}/*.csv"
    df = (spark.read.format("csv")
        .option("header", "True")       # Agar aapki CSV mein pehli line column names hai
        .option("inferSchema", "true")  # Data types automatically samajhne ke liye
        .load(csv_path)
    )
    
    return df



# COMMAND ----------

def writeRawToDeltaLake(df,folder_group,entity):
    delta_path = f"{ADLS_DEV_BASE_PATH}{DELTALAKE_RAW_PATH}{folder_group}/{entity}/"
    df.write.mode("overwrite").option("overwriteSchema","True").option("path",delta_path).save()

# COMMAND ----------

def readFromDeltaPath(folder_group, entity):
    delta_path = f"{ADLS_DEV_BASE_PATH}{DELTALAKE_RAW_PATH}{folder_group}/{entity}/"
    # Dataframe read karein
    df = ( spark.read.format("delta")
        .option("path",delta_path)
        .load()
    )
    
    return df

# COMMAND ----------

#def writeRawToDeltaLake(entityDf,deltaLakePath):
 #   entityDf.write.mode("overwrite").option("overwriteSchema","True").option("path",ADLS_DEV_BASE_PATH + deltaLakePath).save()


# COMMAND ----------

#def readFromDeltaPath(entityName):
 #   df = (spark.read.format("delta")
 #     .option("path",f"{ADLS_DEV_BASE_PATH}/{DELTALAKE_RAW_PATH}{entityName}")
 #     .load()
 #     )
 #   return df

# COMMAND ----------

#def saveDeltaTableToCatalog(df,schema,tableName):
 #   schema = schema.lower()
  #  tableName = tableName.lower()
   # spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
   # df.write.format("delta").mode("overwrite").saveAsTable(f"{schema}.{tableName}")


# COMMAND ----------

def save_to_unity_catalog(df, schema, tablename):
    schema = schema.lower()
    tableName = tablename.lower()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    full_table_path = f"90111adbdev.{schema}.{tableName}"
    df.write .format("delta") .mode("overwrite") .saveAsTable(full_table_path)