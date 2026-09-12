# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
server_name = "jdbc:sqlserver://cudclouddev.database.windows.net"
database_name = "cudclouddev"
jdbcurl = server_name + ";" + "databaseName=" + database_name + ";"

# COMMAND ----------

sqlusername = dbutils.secrets.get("adbdevscope","sqlusername")
sqlpassword = dbutils.secrets.get("adbdevscope","sqlpassword")



# COMMAND ----------

def ReadTableFromDatabase(Tablename):
    try:
        df = (spark.read.format("jdbc")
        .option("url",jdbcurl)
        .option("username",sqlusername)
        .option("password",sqlpassword)
        .option("dbtable",Tablename).load()
        )
    except Exception as e:
        raise Exception    
    return df

# COMMAND ----------

def QueryFromDatabase(sqlquery):
    df = (spark.read.format("jdbc")
    .option("url",jdbcurl)
    .option("username",sqlusername)
    .option("password",sqlpassword)
    .option("query",sqlquery).load()
    )
    return df


# COMMAND ----------

def WriteDataframeToDatabase(dfName,Tablename):
    (dfName.write
    .format("jdbc")
    .option("url", jdbcurl) 
    .option("dbtable", Tablename)
    .option("user", sqlusername) 
    .option("password", sqlpassword) 
    .save()
        )

# COMMAND ----------

def WriteDataframeToDatabaseOverwrite(dfName,Tablename):
    (dfName.write.format("jdbc")
        .option("url",jdbcurl)
        .option("username",sqlusername)
        .option("password",sqlpassword)
        .mode("overwrite")
        .option("dbtable",Tablename).save()
        )

# COMMAND ----------

def WriteDataframeToDatabaseMode(dfName,Tablename,writemode):
    (dfName.write.format("jdbc")
        .option("url",jdbcurl)
        .option("username",sqlusername)
        .option("password",sqlpassword)
        .mode(writemode)
        .option("dbtable",Tablename).save()
        )