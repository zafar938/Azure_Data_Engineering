# Databricks notebook source
# MAGIC %md ###Run Shared Libraries

# COMMAND ----------

# MAGIC %run ../Misc/SharedLibraries

# COMMAND ----------

UpdatedDateTime = datetime.datetime.now()
Entity = "dimpromotable"

# COMMAND ----------

# MAGIC %md ###Read Bronze tables
# MAGIC

# COMMAND ----------

promotablebronzedf= spark.table("bronze.promotable")


# COMMAND ----------

# MAGIC %md ###Build Dimension/Fact table
# MAGIC

# COMMAND ----------

promotabledf = promotablebronzedf.filter(promotablebronzedf.RecordId.isNotNull()
    ).select(
        promotablebronzedf.PromotionId,
        F.when(promotablebronzedf.LastProcessedChange_DateTime.isNull(), "1900-01-01").otherwise(promotablebronzedf.LastProcessedChange_DateTime).cast("timestamp").alias("LastProcessedChange_DateTime"),
        F.from_utc_timestamp(promotablebronzedf.DataLakeModified_DateTime,'CST').alias("DataLakeModified_DateTime"),
        F.trim(promotablebronzedf.PromotionName).alias("PromotionName"),
        F.trim(promotablebronzedf.PromoCode).alias("PromoCode"),
        F.trim(promotablebronzedf.PromoType).alias("PromoType"),
        promotablebronzedf.PromoPercentage,
        F.from_utc_timestamp(promotablebronzedf.ValidFrom,'CST').alias("ValidFrom"),
        F.from_utc_timestamp(promotablebronzedf.ValidTo,'CST').alias("ValidTo"),
        promotablebronzedf.IsActive,
        promotablebronzedf.RecordId.alias("PromoRecordId")
    ).withColumn("UpdatedDateTime", F.lit(UpdatedDateTime)
    ).withColumn("PartyHashKey", F.xxhash64("PromoRecordId")
    )
display(promotabledf)

# COMMAND ----------

# MAGIC %md ###Final dataframe
# MAGIC

# COMMAND ----------

df_final = promotabledf

# COMMAND ----------

# MAGIC %md ## Write to Silver Schema

# COMMAND ----------

save_to_unity_catalog(df_final,"silver",Entity)