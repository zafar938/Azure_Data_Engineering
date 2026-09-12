# Databricks notebook source
# MAGIC %md
# MAGIC ###Run SharedLibraries 

# COMMAND ----------

# MAGIC %run ../Misc/SharedLibraries

# COMMAND ----------

# MAGIC %md
# MAGIC ###Set Variable

# COMMAND ----------

entity='SalesOrderLine'
folder_group= 'Sales'

# COMMAND ----------

# MAGIC %md
# MAGIC ###Read From Delta Raw Path

# COMMAND ----------

df_delta = readFromDeltaPath(folder_group,entity)
display(df_delta)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Save to Bronze schema

# COMMAND ----------

save_to_unity_catalog(df_delta,"Bronze",entity)