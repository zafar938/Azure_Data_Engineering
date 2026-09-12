# Databricks notebook source
# MAGIC %md ###Run Shared Libraries

# COMMAND ----------

# MAGIC %run ../Misc/SharedLibraries

# COMMAND ----------

UpdatedDateTime = datetime.datetime.now()
Entity = "dimpaymenttypes"

# COMMAND ----------

# MAGIC %md ###Read Bronze tables
# MAGIC

# COMMAND ----------

salesorderlinedf= spark.table("bronze.salesorderline")
display(salesorderlinedf)


# COMMAND ----------

# MAGIC %md ###Create silver dimension table
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.dimpaymenttypes(
# MAGIC   PaymentTypeId INT,
# MAGIC   PaymentTypeDesc STRING
# MAGIC )

# COMMAND ----------

# MAGIC %md ###Build Dimension/Fact table
# MAGIC

# COMMAND ----------

df = salesorderlinedf.select("PaymentTypeDesc").distinct()
display(df)

# COMMAND ----------

paymenttypedf = spark.table("silver.dimpaymenttypes")
display(paymenttypedf)

# COMMAND ----------

newrowsdf=df.exceptAll(paymenttypedf.select("PaymentTypeDesc"))
display(newrowsdf)

# COMMAND ----------

maxdf = spark.sql("select ifnull(max(PaymentTypeId),0) as maxid from {df}",df=paymenttypedf)
toprow = maxdf.head(1)
maxid = toprow[0][0]
print(maxid)

# COMMAND ----------

import pyspark.sql.window as W

# COMMAND ----------

idsdf = newrowsdf.withColumn("PaymentTypeId", F.row_number().over(window=W.Window.orderBy(F.col("PaymentTypeDesc"))))
display(idsdf)

# COMMAND ----------

idsFinal = idsdf.withColumn("PaymentTypeId", F.col("PaymentTypeId")+maxid)
display(idsFinal)

# COMMAND ----------

# MAGIC %sql
# MAGIC select  * from silver.dimpaymenttypes

# COMMAND ----------

# MAGIC %md ###Final dataframe
# MAGIC

# COMMAND ----------

df_final = idsFinal

# COMMAND ----------

# MAGIC %md ## Write to Silver Schema

# COMMAND ----------

save_to_unity_catalog(df_final,"silver",Entity)

# COMMAND ----------

# MAGIC %sql
# MAGIC select  * from silver.dimpaymenttypes