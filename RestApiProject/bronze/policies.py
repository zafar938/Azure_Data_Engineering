# Databricks notebook source
# DBTITLE 1,Import Utility Functions
# MAGIC %run ../misc/Utilities

# COMMAND ----------

# DBTITLE 1,Ingest Insurance Policies from REST API to Bronze Layer
# ============================================
# Bronze Layer Ingestion: Insurance Policies
# ============================================
# This cell fetches insurance policy data from a REST API and loads it
# into the bronze layer (raw data zone) of the medallion architecture.
#
# Process:
# 1. Fetch data from REST API endpoint for insurance_policies dataset
# 2. Parse and convert date columns (start_date, end_date) to proper date types
# 3. Write the raw data to the bronze Delta table
# 4. Log success/failure status for monitoring and auditing

# Initialize error tracking
error_msg = None
dataset_name = "insurance_policies"

try:
    # ----------------------------------------
    # Step 1: Fetch data from REST API
    # ----------------------------------------
    # The fetch_rest_api_dataset function:
    #   - Makes HTTP request to the REST API endpoint
    #   - Returns a Spark DataFrame with the response data
    #   - Automatically parses specified columns as date types
    df = fetch_rest_api_dataset(
        f"{dataset_name}",
        date_columns=["start_date", "end_date"]  # Columns to parse as dates
    )
    
    # ----------------------------------------
    # Step 2: Write to Bronze Delta Table
    # ----------------------------------------
    # The writeDfToTable function:
    #   - Creates the bronze schema if it doesn't exist
    #   - Writes DataFrame to: insureallbi.bronze.insurance_policies
    #   - Uses Delta Lake format for ACID transactions
    #   - Appends new data or overwrites based on configuration
    writeDfToTable(df, "bronze", dataset_name)
    
    print(f"✓ Successfully ingested {dataset_name} to bronze layer")

except Exception as e:
    # ----------------------------------------
    # Error Handling and Logging
    # ----------------------------------------
    # If ingestion fails:
    #   1. Capture the error message
    #   2. Log failure status to monitoring table
    #   3. Print failure message
    #   4. Re-raise exception to stop pipeline execution
    status = "FAILED"
    error_msg = str(e)
    
    # Log the failure for monitoring and alerting
    log_pipeline_status("bronze", dataset_name, error_msg)
    
    print(f"✗ Failed to read {dataset_name}")
    print(f"Error: {error_msg}")
    
    # Re-raise to prevent downstream processing of incomplete data
    raise Exception(error_msg)

# COMMAND ----------

# DBTITLE 1,Verify Bronze Table Data
# MAGIC %sql
# MAGIC -- ============================================
# MAGIC -- Data Verification: View Bronze Table Sample
# MAGIC -- ============================================
# MAGIC -- This query retrieves a sample of records from the bronze layer
# MAGIC -- insurance_policies table to verify successful data ingestion.
# MAGIC --
# MAGIC -- Purpose:
# MAGIC --   - Quick visual inspection of ingested data
# MAGIC --   - Verify column names and data types
# MAGIC --   - Check for data quality issues in raw data
# MAGIC --   - Confirm date parsing was successful
# MAGIC --
# MAGIC -- Table: insureallbi.bronze.insurance_policies
# MAGIC -- Expected columns: policy_code, name, description, category,
# MAGIC --                   coverage, coverage_type, base_premium_usd,
# MAGIC --                   currency, term_period, start_date, end_date,
# MAGIC --                   is_active, and technical columns (_index, _source_file)
# MAGIC
# MAGIC SELECT * 
# MAGIC FROM insureallbi.bronze.insurance_policies 
# MAGIC LIMIT 10;