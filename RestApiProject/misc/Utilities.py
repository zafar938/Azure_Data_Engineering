# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
import requests
import time
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Row

# COMMAND ----------

# DBTITLE 1,fetch_rest_api_dataset
def fetch_rest_api_dataset(dataset_name, username="cduuser", password="Rest@1234", per_page=1000,date_columns=None):
    import base64
    """
    Fetches ALL pages of a WordPress REST dataset and returns a Spark DataFrame.
    
    Args:
        dataset_name (str): API dataset endpoint (e.g., insurance_claims)
        username (str): Basic Auth username
        password (str): Basic Auth password
        per_page (int): Records per page (default 100)
        
    Returns:
        Spark DataFrame
    """
    
    base_url = f"https://cloudanddatauniverse.com/wp-json/custom-api/datasets/{dataset_name}"

    # ---- Build Authorization Header ----
    credentials = f"{username}:{password}"
    token = base64.b64encode(credentials.encode()).decode()
    headers = {"Authorization": f"Basic {token}"}

     # ---- Smart request function (inline) ----
    def make_request(url):
        attempt = 1
        max_attempts = 3

        while attempt <= max_attempts:
            #response = requests.get(url, headers=headers)
            response = requests.get(url, headers=headers, timeout=120)

            try:
                #data = response.json()
                import json
                data = json.loads(response.text, strict=False)
            except Exception as e:
               # raise Exception(f"Invalid response: {response.text}")
                raise Exception(f"JSON Parse Error: {str(e)}. Response snippet: {response.text[-250:]}")

            # ✅ Success
            if response.status_code == 200:
                return data

            # 🚫 Rate limit handling
            if response.status_code == 429 or data.get("code") == "rate_limit_exceeded":

                if attempt == 1:
                    print("⚠️ Rate limit hit. Retrying in 1 minute...")
                    time.sleep(60)

                elif attempt == 2:
                    print("⏳ Still rate limited. Waiting 5 minutes...")
                    time.sleep(300)

                else:
                    raise Exception("❌ Rate limit persists after retries. Failing pipeline.")

                attempt += 1
                continue

            # ❌ Other errors
            raise Exception(f"API Error: {data}")
        
    # ---- Get first page metadata ----
    first_url = f"{base_url}?page=1&per_page={per_page}"
    first_response = make_request(first_url)

    if "total_pages" not in first_response:
        raise Exception(f"Unexpected API response: {first_response}")

    total_pages = first_response["total_pages"]
    print(f"Dataset: {dataset_name} → Total pages = {total_pages}")

    # ---- Loop through all pages ----
    all_rows = []

    for page in range(1, total_pages + 1):
        url = f"{base_url}?page={page}&per_page={per_page}"
        
        response =make_request(url)
        
        if "data" not in response:
            print(f"❌ Error on page {page}: {response}")
            continue

        rows = response["data"]
        all_rows.extend(rows)

        print(f"✔ Fetched page {page}/{total_pages}")

    # ---- Convert to Spark DataFrame ----
    raw_df = spark.createDataFrame(all_rows)
    clean_df = clean_dataset(raw_df)
    typed_df = apply_schema(clean_df,dataset_name)
    return typed_df

# COMMAND ----------

def clean_dataset(df,date_columns=None):

    # 1. Trim strings
    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(c, trim(col(c)))

    # 2. Standardize nulls
    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(
                c,
                when(col(c).isin("", " ", "null", "N/A"), None)
                .otherwise(col(c))
            )

    # 3. Remove duplicates
    df = df.dropDuplicates()

    
   # 4. Cast date column
    if date_columns:
        for col_name in date_columns:
            if col_name in df.columns:
                print(f"Casting column : {col_name}")
                df=df.withColumn(col_name, col(col_name).try_cast('timestamp'))

    return df


# COMMAND ----------

schema_registry = {
    
    "insurance_policies": {
        "policy_code": StringType(),
        "name": StringType(),
        "category": StringType(),
        "base_premium_usd": DoubleType(),
        "coverage": StringType(),
        "currency": StringType(),     
        "coverage_type": StringType(),   
        "term_period": StringType(),
        "description": StringType(),
        "start_date":TimestampType(),
        "end_date":TimestampType(),
        "is_active": BooleanType(),
        "_index": LongType()
    },

      "insurance_agents": {
        "agent_id": IntegerType(),
        "agent_name": StringType(),
        "date_of_joining":TimestampType(),
        "experience_years": IntegerType(),  
        "region_id  ": IntegerType(),
        "id_deleted":BooleanType(),
        "agent_email":StringType(),
        "agent_phone":StringType(),
        "agent_gender":StringType(),
        "date_of_birth":StringType(),
        "agent_address":StringType(),
        "city":StringType(),
        "state":StringType(),
        "country":StringType(),
        "agent_type":StringType(),
        "license_number":StringType(),
        "license_expiry_date":StringType(),
        "agent_status":StringType(),
        "total_policies_sold":IntegerType(),
        "total_commission_earned":DecimalType(),
        "rating":DecimalType(),
        "manager_agent_id":IntegerType(),
        "branch_id":IntegerType(),
        "branch_name":StringType(),
        "zone":StringType(),
        "sales_team":StringType(),
        "policies_sold_current_year":IntegerType(),
        "policies_sold_last_year":IntegerType(),
        "avg_policy_value":DecimalType(),
        "conversion_rate":DecimalType(),
        "customer_retention_rate":DecimalType(),
        "commission_rate":DecimalType(),
        "commission_paid_ytd":DecimalType(),
        "commission_pending":DecimalType(),
        "last_commission_date":StringType(),
        "kyc_verified":BooleanType(),
        "background_check_status":StringType(),
        "compliance_score":DecimalType(),
        "last_audit_date":StringType(),
        "last_login_channel":StringType(),
        "login_count_30_days":IntegerType(),
        "last_activity_timestamp":StringType(),
        "device_type":StringType(),
        "avg_response_time_minutes":IntegerType(),
        "complaints_handled":IntegerType(),
        "escalations_count":DecimalType(),
        "customer_satisfaction_score":StringType(),
        "record_created_timestamp":StringType(),
        "record_updated_timestamp":StringType(),
        "batch_id":StringType()
    },
      
    "insurance_customers": {
        "customer_id": IntegerType(),
        "name": StringType(),
        "dob": StringType(),
        "gender": StringType(),
        "occupation": StringType(),
        "address": StringType(),
        "city": StringType(),
        "state": StringType(),
        "country": StringType(),
        "pincode": IntegerType(),
        "email": StringType(),
        "phone": StringType(),
        "Channel": StringType(),
        "nominated": StringType(),
        "nominee_relation": StringType(),
        "start_date": TimestampType(),
        "end_date": TimestampType(),
        "is_active":BooleanType()
    },

    "insurance_countries": {
        "country_id": IntegerType(),
        "country_name": StringType(),
        "_index": LongType()
    },

    "payment_frequency": { 
        "customer_id": IntegerType(),
        "payment_frequency": StringType(),
        "start_date": TimestampType(),
        "end_date": TimestampType(),
        "_index": IntegerType()
    },
    
    "insurance_payments": {
        "customer_id": IntegerType(),
        "policy_id": StringType(),
        "payment_date": TimestampType(),
        "payment_amount": DoubleType(),
        "payment_frequency": StringType(),
        "payment_mode": StringType(),
        "payment_status": StringType(),
        "transaction_id": StringType(),
        "_index": LongType()
    },

    "insurance_claims": {
        "claim_id": StringType(),
        "customer_id": IntegerType(),
        "policy_id": StringType(),
        "claim_date":TimestampType(),
        "incident_date":TimestampType(),
        "claim_amount": DoubleType(),
        "claim_status": StringType(),
        "approval_date": StringType(),
        "settlement_amount": StringType(),
        "fraud_flag": StringType(),
        "channel":StringType(),
        "reported_delay_days":IntegerType(),
        "_index": LongType()
    },

     "customer_policies": {
        "customer_id": IntegerType(),
        "policy_id": StringType(),
        "policy_enroll_date": TimestampType(),
        "_index": LongType()
    },
}


# COMMAND ----------

def apply_schema(df, dataset_name):
    """
    Casts dataframe columns based on schema registry.
    """
    if dataset_name not in schema_registry:
        raise Exception(f"Schema not found for dataset: {dataset_name}")

    schema = schema_registry[dataset_name]

    casted_df = df
    for col_name, col_type in schema.items():
        if col_name in casted_df.columns:
            casted_df = casted_df.withColumn(col_name, col(col_name).cast(col_type))
    
    return casted_df


# COMMAND ----------

def log_pipeline_status(schemaname,tablename,  error_msg):
    schema = StructType([
    StructField("schemaname", StringType(), True),
    StructField("tablename", StringType(), True),
    StructField("error_message", StringType(), True)
])
    log_df = spark.createDataFrame([
    (schemaname, tablename,  error_msg)], schema=schema).withColumn("log_captured",current_timestamp())
    log_df.write.mode("append").saveAsTable("insureallBI.logs.pipelineruns")

# COMMAND ----------

def writeDfToTable(df,schemaname,tablename):
    df.write.mode("overwrite").option("overwriteSchema","true").saveAsTable(f"insureallBI.{schemaname}.{tablename}")

# COMMAND ----------

def loadIncrementalData(df,schemaName, tableName, mergeKey):
    if not spark.catalog.tableExists(f"insureallBI.{schemaName}.{tableName}"):
        writeDfToTable(df,schemaName,tableName)
    else:
        
        # Build column list for UPDATE and INSERT (excluding merge key as it's in condition)
        columns = [col for col in df.columns if col != mergeKey]
        
        # Build UPDATE SET clause
        update_set_clause = ",\n        ".join([f"target.{col} = source.{col}" for col in columns])
        
        # Build INSERT VALUES clause
        insert_columns = ", ".join(df.columns)
        insert_values = ", ".join([f"source.{col}" for col in df.columns])
        
        # Create temp view
        df.createOrReplaceTempView(f"vw_{tableName}")

        # Execute MERGE statement
        merge_sql = f"""
        MERGE INTO insureallBI.{schemaName}.{tableName} AS target
        USING vw_{tableName} AS source
        ON target.{mergeKey} = source.{mergeKey}
        WHEN MATCHED THEN
            UPDATE SET
                {update_set_clause}
        WHEN NOT MATCHED THEN
            INSERT ({insert_columns})
            VALUES ({insert_values})
        """
        
        print(merge_sql)

        spark.sql(merge_sql,df=df)
            