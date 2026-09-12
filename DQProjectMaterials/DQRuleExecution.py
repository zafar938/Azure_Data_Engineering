# Databricks notebook source
# MAGIC %run ./Utilities
# MAGIC

# COMMAND ----------

# MAGIC %run ./DQChecks

# COMMAND ----------

# DBTITLE 1,Read Rules Metadata
df_rules = ReadTableFromDatabase("dqr.Vw_Rules")
display(df_rules)

# COMMAND ----------


for row in df_rules.collect():
    object_name = row["dqobjectname"]
    sourcelayer = row["sourcelayer"]
    targetlayer = row["targetlayer"]
    rulename   = row["dqrulename"]
    query     = row["sqlquery"]
    sourceattribute     = row["dqattribute1"]
    targetattribute     = row["dqattribute2"]
    
    
    match rulename:
        case "PrimaryKey":
            executePrimaryKeyCheck( object_name,sourcelayer,rulename,sourceattribute,query)
        case "NullCheck":
            executeNullCheck( object_name,sourcelayer,rulename,sourceattribute,query)
        case _:
            print(f"⚠️ Unknown rule type: {rulename}")


# COMMAND ----------

df_watermark = QueryFromDatabase("select watermarkvalue from dqr.incremental_load_mappings where tablename ='dqr.dqresults'")
watermarkvalue= df_watermark.collect()[0][0]
print(watermarkvalue)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType

schema = StructType([
    StructField("objectname", StringType(), True),
    StructField("sourcelayer", StringType(), True),
    StructField("rulename", StringType(), True),
    StructField("sourceresult", DecimalType(), True),
    StructField("rundatetime", TimestampType(), True),
  
])

df_dq_results = (spark.read.format("csv")
    .option("header", "True")
    .schema(schema)
    .option("recursiveFileLookup", "true")
    .option("path", "/Volumes/90111adbdev/dataquality/dqcheckresults/").load())
df_dq_results_final = df_dq_results.filter(f"rundatetime > '{watermarkvalue}'")
display(df_dq_results_final)

# COMMAND ----------

WriteDataframeToDatabaseMode(df_dq_results_final,"dqr.dqresults","append")

# COMMAND ----------

