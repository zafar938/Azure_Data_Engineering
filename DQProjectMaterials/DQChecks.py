# Databricks notebook source
def executePrimaryKeyCheck(objectname, layer, rulename, dqattribute1, sqlquery):
    sqlquery_object = sqlquery.replace(objectname, f"90111adbdev.{layer}.{objectname}")
    df_dqcheck = spark.sql(sqlquery_object)
    if df_dqcheck.isEmpty():           
        df_dqcheck_result = spark.sql(f"SELECT '{objectname}' as objectname , '{layer}' as sourcelayer , 'PrimaryKeyCheck' as rulename , 1 as sourceresult, current_timestamp() as rundatetime") #rule passed
        display(df_dqcheck_result)
    else:
        df_dqcheck_result = spark.sql(f"SELECT '{objectname}' as objectname, '{layer}' as sourcelayer , 'PrimaryKeyCheck' as rulename , 0 as sourceresult,current_timestamp() as rundatetime") #rule failed
        
        display(df_dqcheck_result)
        #write bad records
        spark.table(f"90111adbdev.{layer}.{objectname}").join(df_dqcheck,dqattribute1,"inner").write.mode("append").format("csv").option("header","True").option("path",f"/Volumes/90111adbdev/dataquality/dqcheckbadrecords/{objectname}/{rulename}/").save()

    #write dq result
    df_dqcheck_result.write.mode("append").format("csv").option("header","True").option("path",f"/Volumes/90111adbdev/dataquality/dqcheckresults/{objectname}/{rulename}/").save()

# COMMAND ----------

def executeNullCheck(objectname, layer, rulename, dqattribute1, sqlquery):
    sqlquery_object = sqlquery.replace(objectname, f"90111adbdev.{layer}.{objectname}")
    df_dqcheck = spark.sql(sqlquery_object)
    if df_dqcheck.isEmpty():           
        df_dqcheck_result = spark.sql(f"SELECT '{objectname}' as objectname, '{layer}' as sourcelayer , 'NullCheck' as rulename , 1 as sourceresult, current_timestamp() as rundatetime") #rule passed
        display(df_dqcheck_result)
    else:
        df_dqcheck_result = spark.sql(f"SELECT '{objectname}' as objectname, '{layer}' as sourcelayer , 'NullCheck' as rulename , 0 as sourceresult,current_timestamp() as rundatetime") #rule failed
        
        display(df_dqcheck_result)
        #write bad records
        df_dqcheck_badrecords = spark.sql(f"SELECT * FROM 90111adbdev.{layer}.{objectname} WHERE {dqattribute1} IS NULL")
        df_dqcheck_badrecords.write.mode("append").format("csv").option("header","True").option("path",f"/Volumes/90111adbdev/dataquality/dqcheckbadrecords/{objectname}/NullCheck/").save()

    #write dq result
    df_dqcheck_result.write.mode("append").format("csv").option("header","True").option("path",f"/Volumes/90111adbdev/dataquality/dqcheckresults/{objectname}/NullCheck/").save()

# COMMAND ----------

# executeNullCheck("costcenter","bronze","Nullcheck","LastProcessedChange_DateTime","SELECT  COUNT(*) AS DuplicateCount FROM costcenter where LastProcessedChange_DateTime IS NULL;")

# COMMAND ----------

# display(spark.read.format("csv").option("header","True").load("/Volumes/90111adbdev/dataquality/dqcheckresults/costcenter/NullCheck/"))

# COMMAND ----------

# display(spark.read.format("csv").load("/Volumes/90111adbdev/dataquality/dqcheckbadrecords/costcenter/NullCheck/"))

# COMMAND ----------

