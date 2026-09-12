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
Entity='dimvendor'

# COMMAND ----------

# MAGIC %md
# MAGIC ###Read Bronze Tables

# COMMAND ----------

vendorDf=spark.table('`90111adbdev`.bronze.vendtable')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Build Dimension\Fact Table

# COMMAND ----------

dimvendor_df = vendorDf.filter(vendorDf.RecordId.isNotNull())\
              .select(
                     vendorDf.VendorId.alias('VendorId'),
                     F.trim(vendorDf.VendorName).alias('VendorName'),
                     F.when(vendorDf.LastProcessedChange_DateTime.isNull(),"1900-01-01").otherwise(vendorDf.LastProcessedChange_DateTime).cast('timestamp').alias('LastProcessedChange_DateTime'),
                     F.from_utc_timestamp(vendorDf.DataLakeModified_DateTime, 'CST').alias('DataLakeModified_DateTime'),
                     F.trim(vendorDf.Address).alias('Address'),
                     F.trim(vendorDf.City).alias('City'),
                     F.trim(vendorDf.State).alias('State'),
                     F.trim(vendorDf.Country).alias('Country'),
                     F.trim(vendorDf.ZipCode).alias('ZipCode'),
                     F.trim(vendorDf.Region).alias('Region'),
                     F.from_utc_timestamp(vendorDf.ValidFrom,'CST').alias('ValidFrom'),
                     F.from_utc_timestamp(vendorDf.ValidTo,'CST').alias('ValidTo'),
                     vendorDf.Active.alias('Active'),
                     vendorDf.RecordId.alias('VendorRecordId'),
                     F.trim(vendorDf.TaxId).alias('TaxId'),
                     F.trim(vendorDf.CurrencyCode).alias('CurrencyCode')
              ).withColumn('UpdatedDateTime',F.lit(UpdatedDateTime))\
               .withColumn("PartyHasKey", F.xxhash64("VendorRecordId"))\
               .withColumn("VendorDiscount",F.when(F.col("Country") == "US",F.lit(0.01)).when(F.col("Country") == "UK",F.lit(0.006)).otherwise(F.lit(0)))
display(dimvendor_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###final Dataframe

# COMMAND ----------

df_final=dimvendor_df

# COMMAND ----------

# MAGIC %md
# MAGIC ###Write to Silver Schema

# COMMAND ----------

save_to_unity_catalog(df_final,'silver',Entity)