# Databricks notebook source
# MAGIC %md ###Run Shared Libraries

# COMMAND ----------

# MAGIC %run ../Misc/SharedLibraries

# COMMAND ----------

UpdatedDateTime = datetime.datetime.now()
Entity = "dimcusttable"

# COMMAND ----------

# MAGIC %md ###Read Bronze tables
# MAGIC

# COMMAND ----------

custtablebronzedf= spark.table("bronze.custtable")


# COMMAND ----------

# MAGIC %md ###Build Dimension/Fact table
# MAGIC

# COMMAND ----------

custtabledfdf = custtablebronzedf.filter(custtablebronzedf.RecordId.isNotNull()
    ).select(
        custtablebronzedf.CustomerId,
        F.when(custtablebronzedf.LastProcessedChange_DateTime.isNull(), "1900-01-01").otherwise(custtablebronzedf.LastProcessedChange_DateTime).cast("timestamp").alias("LastProcessedChange_DateTime"),
        F.from_utc_timestamp(custtablebronzedf.DataLakeModified_DateTime,'CST').alias("DataLakeModified_DateTime"),
        F.trim(custtablebronzedf.CustomerName).alias("CustomerName"),
        F.trim(custtablebronzedf.Email).alias("Email"),
        F.trim(custtablebronzedf.Phone).alias("Phone"),
        F.trim(custtablebronzedf.Address).alias("Address"),
        F.trim(custtablebronzedf.City).alias("City"),
        F.trim(custtablebronzedf.State).alias("State"),
        F.trim(custtablebronzedf.Country).alias("Country"),
        F.trim(custtablebronzedf.Country).alias("ZipCode"),
        F.trim(custtablebronzedf.Region).alias("Region"),
        F.from_utc_timestamp(custtablebronzedf.SignupDate,'CST').alias("SignupDate"),
        custtablebronzedf.RecordId.alias("CustRecordId")
    ).withColumn("UpdatedDateTime", F.lit(UpdatedDateTime)
    ).withColumn("PartyHashKey", F.xxhash64("CustRecordId")
    )
display(custtabledfdf)

# COMMAND ----------

# MAGIC %md ###Final dataframe
# MAGIC

# COMMAND ----------

df_final = custtabledfdf

# COMMAND ----------

# MAGIC %md ## Write to Silver Schema

# COMMAND ----------

save_to_unity_catalog(df_final,"silver",Entity)

# COMMAND ----------

