# Databricks notebook source
# MAGIC %md ###Run Shared Libraries

# COMMAND ----------

# MAGIC %run ../Misc/SharedLibraries

# COMMAND ----------

UpdatedDateTime = datetime.datetime.now()
Entity = "dimvertical"

# COMMAND ----------

# MAGIC %md ###Read Bronze tables
# MAGIC

# COMMAND ----------

workerdf= spark.table("bronze.workertable")


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.dimvertical (
# MAGIC   VerticalId BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   Vertical STRING
# MAGIC )

# COMMAND ----------

# MAGIC %md ###Build Dimension/Fact table
# MAGIC

# COMMAND ----------

df = workerdf.select(F.expr("trim(Vertical) AS Vertical")).distinct()
display(df)

# COMMAND ----------

verticaldf = spark.table("silver.dimvertical")
display(verticaldf)

# COMMAND ----------

newrowsdf=df.filter(F.col("Vertical").isNotNull()).exceptAll(verticaldf.select("Vertical"))
display(newrowsdf)

# COMMAND ----------

spark.sql("insert into silver.dimvertical(vertical) select  Vertical from {newrowsdf}",newrowsdf=newrowsdf)

# COMMAND ----------

display(spark.table("silver.dimvertical"))

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO bronze.workertable(Vertical)VALUES("Data & AI")

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM bronze.workertable WHERE Vertical ="Data & AI"