# Databricks notebook source
# MAGIC %md ###Run Shared Libraries

# COMMAND ----------

# MAGIC %run ../Misc/SharedLibraries

# COMMAND ----------

UpdatedDateTime = datetime.datetime.now()
Entity = "dimpurchasecategory"

# COMMAND ----------

# MAGIC %md ###Read Bronze tables
# MAGIC

# COMMAND ----------

purchaseCategoryDf= spark.table("bronze.purchcategory")


# COMMAND ----------

# MAGIC %md ###Build Dimension/Fact table
# MAGIC

# COMMAND ----------

dimpurchaseCategoryDf = purchaseCategoryDf.filter(purchaseCategoryDf.RecordId.isNotNull()
    ).select(
       purchaseCategoryDf.CategoryId,
       F.trim(purchaseCategoryDf.CategoryName).alias("CategoryName"),
       F.when(purchaseCategoryDf.LastProcessedChange_DateTime.isNull(), "1900-01-01").otherwise(purchaseCategoryDf.LastProcessedChange_DateTime).cast("timestamp").alias("LastProcessedChange_DateTime"),
       F.from_utc_timestamp(purchaseCategoryDf.DataLakeModified_DateTime,'CST').alias("DataLakeModified_DateTime"),
       purchaseCategoryDf.CategoryGroupId,
       purchaseCategoryDf.RecordId.alias("PurchCategoryRecordId"),       
    ).withColumn("UpdatedDateTime", F.lit(UpdatedDateTime)
    ).withColumn("PurchCategoryHashKey", F.xxhash64("PurchCategoryRecordId")
    )
display(dimpurchaseCategoryDf)

# COMMAND ----------

# MAGIC %md ###Final dataframe
# MAGIC

# COMMAND ----------

df_final = dimpurchaseCategoryDf

# COMMAND ----------

# MAGIC %md ## Write to Silver Schema

# COMMAND ----------

save_to_unity_catalog(df_final,"silver",Entity)

# COMMAND ----------

