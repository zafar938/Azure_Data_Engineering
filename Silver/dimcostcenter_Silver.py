# Databricks notebook source
# MAGIC %md ###Run Shared Libraries

# COMMAND ----------

# MAGIC %run ../Misc/SharedLibraries

# COMMAND ----------

UpdatedDateTime = datetime.datetime.now()
Entity = "dimcostcenter"

# COMMAND ----------

# MAGIC %md ###Read Bronze tables
# MAGIC

# COMMAND ----------

costcenterDf= spark.table("bronze.CostCenter")


# COMMAND ----------

# MAGIC %md ###Build Dimension/Fact table
# MAGIC

# COMMAND ----------

dimcostcenterDf = costcenterDf.filter(costcenterDf.RecordId.isNotNull()
    ).select(
        costcenterDf.CostCenterNumber,
        F.when(costcenterDf.LastProcessedChange_DateTime.isNull(), "1900-01-01").otherwise(costcenterDf.LastProcessedChange_DateTime).cast("timestamp").alias("LastProcessedChange_DateTime"),
        F.from_utc_timestamp(costcenterDf.DataLakeModified_DateTime,'CST').alias("DataLakeModified_DateTime"),
        costcenterDf.Vat,
        costcenterDf.RecordId.alias("CostCenterRecordId"),
    ).withColumn("UpdatedDateTime", F.lit(UpdatedDateTime)
    ).withColumn("CostCenterHashKey", F.xxhash64("CostCenterRecordId")
    )
display(dimcostcenterDf)

# COMMAND ----------

# MAGIC %md ###Final dataframe
# MAGIC

# COMMAND ----------

df_final = dimcostcenterDf

# COMMAND ----------

# MAGIC %md ## Write to Silver Schema

# COMMAND ----------

save_to_unity_catalog(df_final,"silver",Entity)