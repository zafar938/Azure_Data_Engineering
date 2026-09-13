# Databricks notebook source
# DBTITLE 1,Run Utilities
# MAGIC %run ../misc/Utilities

# COMMAND ----------

# DBTITLE 1,Set Configs
# ============================================
# Configuration - Update these values
# ============================================
SOURCE_TABLE = "insureallBI.bronze.payment_frequency"  
TARGET_TABLE = "dimpaymentfrequency"

# COMMAND ----------

# DBTITLE 1,Read Bronze Table


# ============================================
# Read Bronze Data
# ============================================
df_payment_frequency = spark.table(SOURCE_TABLE)
display(df_payment_frequency)

# COMMAND ----------

# DBTITLE 1,Final df
df_final = df_payment_frequency

# COMMAND ----------

# DBTITLE 1,Save to Table
writeDfToTable(df_final,"silver",TARGET_TABLE)