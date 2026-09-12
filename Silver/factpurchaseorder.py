# Databricks notebook source
# MAGIC %md ###Run Shared Libraries

# COMMAND ----------

# MAGIC %run ../Misc/SharedLibraries

# COMMAND ----------

UpdatedDateTime = datetime.datetime.now()
Entity = "factpurchaseorder"

# COMMAND ----------

# MAGIC %md ###Read Bronze tables
# MAGIC

# COMMAND ----------

purchaseorderDf= spark.table("90111adbdev.bronze.purchaseorder")
dimcostcenterDf= spark.table("90111adbdev.silver.dimcostcenter")
dimcurrencyDf= spark.table("90111adbdev.silver.dimcurrency")


# COMMAND ----------

# MAGIC %md ###Build Dimension/Fact table
# MAGIC

# COMMAND ----------

factpurchaseorderDf = purchaseorderDf.filter(purchaseorderDf.RecordId.isNotNull()
    ).join(dimcostcenterDf, purchaseorderDf.CostCenter == dimcostcenterDf.CostCenterNumber, "left"
    ).join(dimcurrencyDf, purchaseorderDf.currencycode == dimcurrencyDf.CurrencyCode, "left"
    ).select(
        purchaseorderDf.PoNumber,
        purchaseorderDf.LineItem,
        purchaseorderDf.VendId.alias("VendorKey"),
        F.when(purchaseorderDf.LastProcessedChange_DateTime.isNull(), "1900-01-01").otherwise(purchaseorderDf.LastProcessedChange_DateTime).cast("timestamp").alias("LastProcessedChange_DateTime"),
        F.from_utc_timestamp(purchaseorderDf.DataLakeModified_DateTime,'CST').alias("DataLakeModified_DateTime"),
        purchaseorderDf.Qty,
        purchaseorderDf.PurchasePrice,
        purchaseorderDf.TotalOrder.cast("double").alias("TotalOrder"),
        purchaseorderDf.CostCenter.alias("CostCenterKey"),
        dimcostcenterDf.Vat.alias("VatPercentage"),
        F.round((purchaseorderDf.TotalOrder * dimcostcenterDf.Vat),4).alias("VatAmount"),
        F.round((purchaseorderDf.TotalOrder + (purchaseorderDf.TotalOrder * dimcostcenterDf.Vat)),4).alias("TotalAmount"),
        purchaseorderDf.ExchangeRate,
        purchaseorderDf.Itemkey,
        dimcurrencyDf.CurrencyId.alias("CurrencyKey"),
        F.from_utc_timestamp(purchaseorderDf.OrderDate,'CST').alias("OrderDate"),
        F.from_utc_timestamp(purchaseorderDf.ShipDate,'CST').alias("ShipDate"),
        F.from_utc_timestamp(purchaseorderDf.DeliveredDate,'CST').alias("DeliveredDate"),
        F.date_format(purchaseorderDf.OrderDate,'yyyyMMdd').cast("int").alias("OrderDateKey"),
        F.date_format(purchaseorderDf.ShipDate,'yyyyMMdd').cast("int").alias("ShipDateKey"),
        F.date_format(purchaseorderDf.DeliveredDate,'yyyyMMdd').cast("int").alias("DeliveredDateKey"),
        purchaseorderDf.TrackingNumber,
        purchaseorderDf.Batchid.alias("BatchId"),
        purchaseorderDf.CreatedBy,
        purchaseorderDf.RecordId.alias("PurchaseOrderRecordId"),
        purchaseorderDf.CategoryId.alias("CategoryKey")
    ).withColumn("UpdatedDateTime", F.lit(UpdatedDateTime)
    ).withColumn("PurchaseOrderHashKey", F.xxhash64("PurchaseOrderRecordId")
    )
display(factpurchaseorderDf)

# COMMAND ----------

# MAGIC %md ###Final dataframe
# MAGIC

# COMMAND ----------

df_final = factpurchaseorderDf

# COMMAND ----------

# MAGIC %md ## Write to Silver Schema

# COMMAND ----------

save_to_unity_catalog(df_final,"silver",Entity)