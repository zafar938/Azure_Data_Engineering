# Databricks notebook source
# MAGIC %md ###Run SharedLibraries

# COMMAND ----------

# MAGIC %run ../Misc/SharedLibraries

# COMMAND ----------

# MAGIC %md ###Set Variable

# COMMAND ----------

UpdatedDateTime = datetime.datetime.now()
Entity='Dimparty'

# COMMAND ----------

# MAGIC %md ###Read Bronze Tables

# COMMAND ----------

df_parties=spark.table('90111adbdev.bronze.parties')
df_partyaddress=spark.table('90111adbdev.bronze.partyaddress')

# COMMAND ----------

# MAGIC %md ### Build Dimension\Fact Table

# COMMAND ----------

dimParty_df = df_parties.join(df_partyaddress,df_parties.PartyId==df_partyaddress.PartyNumber,'left')\
              .filter(df_parties.RecordId.isNotNull())\
              .select(
                     df_parties.PartyId.alias('PartyId'),
                     F.trim(df_parties.PartyName).alias('PartyName'),
                     F.when(df_parties.LastProcessedChange_DateTime.isNull(), "1900-01-01").otherwise(df_parties.LastProcessedChange_DateTime).cast("timestamp").alias('LastProcessedChange_DateTime'),
                     F.from_utc_timestamp(df_parties.DataLakeModified_DateTime,'CST').alias('DataLakeModified_DateTime'),
                     df_parties.PartyAddressCode.alias('PartyAddressCode'),
                     F.from_utc_timestamp(df_parties.EstablishedDate,'CST').alias('EstablishedDate'),
                     F.trim(df_parties.PartyEmailId).alias('PartyEmailId'),
                     F.trim(df_parties.PartyContactNumber).alias('PartyContactNumber'),
                     df_parties.RecordId.alias('PartyRecordId'),
                     F.trim(df_parties.TaxId).alias('TaxId'),
                     F.trim(df_partyaddress.Address).alias('Address'),
                     F.trim(df_partyaddress.City).alias('City'),
                     F.trim(df_partyaddress.State).alias('State'),
                     F.trim(df_partyaddress.Country).alias('Country'),
                     F.trim(df_partyaddress.ZipCode).alias('ZipCode'),
                     F.trim(df_partyaddress.Region).alias('Region'),
                     F.from_utc_timestamp(df_partyaddress.ValidFrom,'CST').alias('ValidFrom'),
                     F.when(df_partyaddress.ValidTo.isNull(), "1900-01-01").otherwise(df_partyaddress.ValidTo).cast("timestamp").alias('ValidTo'),
                     df_partyaddress.RecordId.alias('PartyAddressRecordId')
              ).withColumn('UpdatedDateTime',F.lit(UpdatedDateTime))\
               .withColumn("PartyHasKey", F.xxhash64("PartyRecordId"))
display(dimParty_df)

# COMMAND ----------

# MAGIC %md ###final Dataframe

# COMMAND ----------

df_final=dimParty_df

# COMMAND ----------

# MAGIC %md ###Write to Silver Schema

# COMMAND ----------

save_to_unity_catalog(df_final,'silver',Entity)