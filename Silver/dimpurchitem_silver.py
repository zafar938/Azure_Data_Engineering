# Databricks notebook source
# MAGIC %md ###Run Shared Libraries

# COMMAND ----------

# MAGIC %run ../Misc/SharedLibraries

# COMMAND ----------

UpdatedDateTime = datetime.datetime.now()
Entity = "dimpurchitem"

# COMMAND ----------

# MAGIC %md ###Read Bronze tables
# MAGIC

# COMMAND ----------

purchaseItemDf= spark.table("bronze.purchitem")


# COMMAND ----------

# MAGIC %md ###Build Dimension/Fact table
# MAGIC

# COMMAND ----------

dimpurchaseItemDf = purchaseItemDf.filter(purchaseItemDf.RecordId.isNotNull()
    ).select(
       purchaseItemDf.ItemId,
       F.trim(purchaseItemDf.Txt).alias("ProductName"),
       F.when(purchaseItemDf.LastProcessedChange_DateTime.isNull(), "1900-01-01").otherwise(purchaseItemDf.LastProcessedChange_DateTime).cast("timestamp").alias("LastProcessedChange_DateTime"),
       F.from_utc_timestamp(purchaseItemDf.DataLakeModified_DateTime,'CST').alias("DataLakeModified_DateTime"),
       F.from_utc_timestamp(purchaseItemDf.ValidFrom,'CST').alias("ValidFrom"),
       F.from_utc_timestamp(purchaseItemDf.ValidTo,'CST').alias("ValidTo"), 
       purchaseItemDf.Price.alias("ProductPerUnitCost"),
       purchaseItemDf.RecordId.alias("PurchItemRecordId"),
       purchaseItemDf.CategoryID.alias("CategoryID"),       
    ).withColumn("UpdatedDateTime", F.lit(UpdatedDateTime)
    ).withColumn("PurchItemHashKey", F.xxhash64("PurchItemRecordId")
    )
display(dimpurchaseItemDf)

# COMMAND ----------

# MAGIC %md ###Final dataframe
# MAGIC

# COMMAND ----------

df_final = dimpurchaseItemDf

# COMMAND ----------

# MAGIC %md ## Write to Silver Schema

# COMMAND ----------

save_to_unity_catalog(df_final,"silver",Entity)

# COMMAND ----------

