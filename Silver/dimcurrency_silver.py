# Databricks notebook source
# MAGIC %md
# MAGIC ###Run SharedLibraries

# COMMAND ----------

# MAGIC %run ../Misc/SharedLibraries

# COMMAND ----------

# MAGIC %md
# MAGIC ###Set Variable

# COMMAND ----------

UpdatedDateTime = datetime.datetime.now()
Entity='dimcurrency'

# COMMAND ----------

# MAGIC %md
# MAGIC ###Read Bronze Tables

# COMMAND ----------

currencyDf=spark.table('90111adbdev.bronze.currency')


# COMMAND ----------

# MAGIC %md
# MAGIC ### Build Dimension\Fact Table

# COMMAND ----------

dimcurrency_df = currencyDf.filter(currencyDf.RecordId.isNotNull())\
              .select(
                    currencyDf.CurrencyId,
                    currencyDf.Code.alias('CurrencyCode'),
                    F.when(currencyDf.LastProcessedChange_DateTime.isNull(),"1900-01-01").otherwise(currencyDf.LastProcessedChange_DateTime).cast("timestamp").alias('LastProcessedChange_DateTime'),
                    F.from_utc_timestamp(currencyDf.DataLakeModified_DateTime, "CST").alias('DataLakeModified_DateTime'),
                    currencyDf.Country.alias('Country'),
                    currencyDf.CurrencyName.alias('CurrencyName'),
                    currencyDf.RecordId.alias('CurrencyRecordId')
              ).withColumn('UpdatedDateTime',F.lit(UpdatedDateTime))\
               .withColumn("CurrencyHashKey", F.xxhash64("CurrencyRecordId"))
display(dimcurrency_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###final Dataframe

# COMMAND ----------

df_final=dimcurrency_df

# COMMAND ----------

# MAGIC %md
# MAGIC ###Write to Silver Schema

# COMMAND ----------

save_to_unity_catalog(df_final,'silver',Entity)