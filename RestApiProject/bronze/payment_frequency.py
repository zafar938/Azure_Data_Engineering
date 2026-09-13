# Databricks notebook source
# MAGIC %run ../misc/Utilities

# COMMAND ----------

error_msg = None
dataset_name="payment_frequency"

try:
    df = fetch_rest_api_dataset(f"{dataset_name}")
    df.write.mode("overwrite").saveAsTable(f"insureallBI.bronze.{dataset_name}")

except Exception as e:
    status = "FAILED"
    error_msg = str(e)
    log_pipeline_status("bronze", dataset_name,  error_msg)
    print(f"Failed to read {dataset_name}")
    raise Exception(error_msg) 


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from insureallBI.bronze.payment_frequency limit 10