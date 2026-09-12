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
Entity='dimdate'

# COMMAND ----------

# MAGIC %md
# MAGIC ###Read Bronze Tables

# COMMAND ----------

fiscalperioddf=spark.table('90111adbdev.bronze.fiscalperiod')
display(fiscalperioddf)


# COMMAND ----------

# MAGIC %md ###Date Dimension from Python

# COMMAND ----------

start_date = datetime.date(2018,1,1)
end_date = start_date + dateutil.relativedelta.relativedelta(years=8,month=12,day=31)

# COMMAND ----------

start_date = datetime.datetime.strptime(
    f"{start_date}", "%Y-%m-%d"
)
end_date = datetime.datetime.strptime(
    f"{end_date}", "%Y-%m-%d"
)
print(start_date)
print(end_date)

# COMMAND ----------

datepddf = pd.date_range(start_date,end_date, freq='D').to_frame(name='Date')
datedf=spark.createDataFrame(datepddf)
display(datedf)

# COMMAND ----------

joindf = (
    datedf.join(
        fiscalperioddf.filter(fiscalperioddf.RecordId.isNotNull()),
         (datedf.Date >= fiscalperioddf.FiscalStartDate)
        & (datedf.Date <= fiscalperioddf.FiscalEndDate),
        "left",
    ))
display(joindf)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Build Dimension\Fact Table

# COMMAND ----------

datedimdf = joindf.select(
    "Date",
    F.date_format(F.col("Date"), "yyyyMMdd").cast("int").alias("DateId"),
    F.year(F.col("Date")).alias("Year"),
    F.month(F.col("Date")).alias("Month"),
    F.date_format(F.col("Date"), "MMM").cast("string").alias("MonthName"),
    F.dayofmonth(F.col("Date")).alias("Day"),
    F.date_format(F.col("Date"), "E").cast("string").alias("DayName"),
    F.quarter(F.col("Date")).alias("Quarter"),
    F.col("FiscalPeriodName").alias("FiscalPeriodName"),    
    "FiscalStartDate",
    "FiscalEndDate",
    "FiscalMonth",
    "FiscalYearStart",
    "FiscalYearEnd",
    "FiscalQuarter",
    "FiscalQuarterStart",
    "FiscalQuarterEnd",
    F.concat(F.lit("FY"),"FiscalYear").alias("FiscalYear"),
    F.lit(UpdatedDateTime).alias("UpdatedDateTime"),
    F.xxhash64("DateId").alias("DateKey")
)
display(datedimdf)

# COMMAND ----------



duplicates_summary = datedimdf.groupBy("DateId").count().filter(F.col("count") > 1)
display(duplicates_summary)

# COMMAND ----------

clean_datedimdf = datedimdf.dropDuplicates(["DateId"])
display(clean_datedimdf)

# COMMAND ----------

# MAGIC %md
# MAGIC ###final Dataframe

# COMMAND ----------

df_final=clean_datedimdf

# COMMAND ----------

# MAGIC %md
# MAGIC ###Write to Silver Schema

# COMMAND ----------

save_to_unity_catalog(df_final,'silver',Entity)