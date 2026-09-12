# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC ###Run Shared Libraries

# COMMAND ----------

# MAGIC %run ../Misc/SharedLibraries

# COMMAND ----------

# MAGIC %md
# MAGIC ###Define Variables

# COMMAND ----------

entityName = "Parties"
manifest = "Purchase"



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