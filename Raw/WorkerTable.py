# Databricks notebook source
# MAGIC %md
# MAGIC ###Run Shared Libraries

# COMMAND ----------

# MAGIC %run ../Misc/SharedLibraries

# COMMAND ----------

# MAGIC %md
# MAGIC ###Define Variables

# COMMAND ----------


entityName = "WorkerTable"
manifest = "HR"


# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Entity

# COMMAND ----------


partiesdf = readfiles(manifest,entityName)
display(partiesdf)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to Delta Lake

# COMMAND ----------

writeRawToDeltaLake(partiesdf,manifest,entityName)