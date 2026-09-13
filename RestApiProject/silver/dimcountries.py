# Databricks notebook source
# DBTITLE 1,Run Utilities
# MAGIC %run ../misc/Utilities

# COMMAND ----------

# DBTITLE 1,Set Configs
# ============================================
# Configuration - Update these values
# ============================================
SOURCE_TABLE = "insureallBI.bronze.insurance_countries"  
TARGET_TABLE = "dimcountries"

# COMMAND ----------

# DBTITLE 1,Read Bronze Table


# ============================================
# Read Bronze Data
# ============================================
df_country = spark.table(SOURCE_TABLE)
display(df_country)

# COMMAND ----------

# DBTITLE 1,Final df
df_final = df_country

# COMMAND ----------

# DBTITLE 1,Save to Table
writeDfToTable(df_final,"silver",TARGET_TABLE)