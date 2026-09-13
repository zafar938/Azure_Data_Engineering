# Databricks notebook source
# MAGIC %run ../misc/Utilities

# COMMAND ----------

error_msg = None
dataset_name="insurance_countries"

try:
    df = fetch_rest_api_dataset(f"{dataset_name}")
    writeDfToTable(df,"bronze",dataset_name)

except Exception as e:
    status = "FAILED"
    error_msg = str(e)
    log_pipeline_status("bronze", dataset_name,  error_msg)
    print(f"Failed to read {dataset_name}")
    raise Exception(error_msg) 


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from insureallbi.bronze.insurance_countries limit 10

# COMMAND ----------

