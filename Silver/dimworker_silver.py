# Databricks notebook source
# MAGIC %md ###Run Shared Libraries

# COMMAND ----------

# MAGIC %run ../Misc/SharedLibraries

# COMMAND ----------

UpdatedDateTime = datetime.datetime.now()
Entity = "dimworker"

# COMMAND ----------

# MAGIC %md ###Read Bronze tables
# MAGIC

# COMMAND ----------

workerdf= spark.table("bronze.workertable")
verticaldf = spark.table("silver.dimvertical")

# COMMAND ----------

# MAGIC %md ###Build Dimension/Fact table
# MAGIC

# COMMAND ----------

dimworkerdf = workerdf.filter(workerdf.RecordId.isNotNull()
    ).join(
        verticaldf,workerdf.Vertical == verticaldf.Vertical,"left"
    ).select(
       workerdf.WorkerID,
       F.when(workerdf.LastProcessedChange_DateTime.isNull(), "1900-01-01").otherwise(workerdf.LastProcessedChange_DateTime).cast("timestamp").alias("LastProcessedChange_DateTime"),
       F.from_utc_timestamp(workerdf.DataLakeModified_DateTime,'CST').alias("DataLakeModified_DateTime"),
       workerdf.SupervisorId,
       F.trim(workerdf.WorkerName).alias("WorkerName"),
       F.trim(workerdf.WorkerEmail).alias("WorkerEmail"),
       F.trim(workerdf.Phone).alias("Phone"),
       F.from_utc_timestamp(workerdf.DOJ,'CST').alias("DOJ"),
       F.from_utc_timestamp(workerdf.DOL,'CST').alias("DOL"), 
       verticaldf.VerticalId ,
       workerdf.Type,
       workerdf.PayPerAnnum,
       workerdf.Rate,
       workerdf.RecordId.alias("WorkerRecordId")  
    ).withColumn("UpdatedDateTime", F.lit(UpdatedDateTime)
    ).withColumn("WorkerHashKey", F.xxhash64("WorkerRecordId")
    )
display(dimworkerdf)

# COMMAND ----------

# MAGIC %md ###Final dataframe
# MAGIC

# COMMAND ----------

df_final = dimworkerdf

# COMMAND ----------

# MAGIC %md ## Write to Silver Schema

# COMMAND ----------

save_to_unity_catalog(df_final,"silver",Entity)

# COMMAND ----------

