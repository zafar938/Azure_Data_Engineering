# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Run Utilities
# MAGIC %run ../misc/Utilities

# COMMAND ----------

# DBTITLE 1,Set Catalog
# MAGIC %sql
# MAGIC USE CATALOG insureallbi

# COMMAND ----------

# DBTITLE 1,Set Configs
# ============================================
# Configuration - Update these values
# ============================================
SOURCE_TABLE = "insureallBI.bronze.insurance_policies"  
TARGET_TABLE = "insureallBI.silver.dimpolicies"

# COMMAND ----------

# DBTITLE 1,Read Bronze Table


# ============================================
# Read Bronze Data
# ============================================
df_policies = spark.table(SOURCE_TABLE)
display(df_policies)

# COMMAND ----------

# DBTITLE 1,Silver Transformation Logic with SQL
# ============================================
# Silver Transformation Logic using SQL
# ============================================

df_silver_policies = spark.sql("""
SELECT 
    -- Hash Key (Surrogate Key)
    MD5(CONCAT(policy_code, '|', CAST(start_date AS STRING))) as PolicyHashKey,
    
    -- Original columns
    _index as Index,    
    policy_code as PolicyCode,    
    name as PolicyName,    
    description as PolicyDescription,  
    base_premium_usd as BasePremiumUSD,
    category as Category,
    coverage as Coverage,
    coverage_type as CoverageType,
    currency as Currency,  
    start_date as StartDate,
    end_date as EndDate,
    is_active as IsActive,
    term_period as TermPeriod,
    
    
    -- Time-based calculations
    DATEDIFF(
        COALESCE(end_date, current_date()), 
        start_date
    ) as PolicyDurationDays,
    
    YEAR(start_date) as PolicyYear,
    QUARTER(start_date) as PolicyQuarter,
    DATE_FORMAT(start_date, 'MMMM') as PolicyMonthName,
    MONTH(start_date) as PolicyMonth,
    
    -- Financial calculations
    CASE 
        WHEN coverage_type = 'Annual' THEN ROUND(base_premium_usd / 12, 2)
        WHEN coverage_type = 'Per Trip' THEN base_premium_usd
        WHEN coverage_type = 'Lifetime' THEN ROUND(base_premium_usd / 12, 2)
        ELSE base_premium_usd
    END as MonthlyPremium,
    
    ROUND(coverage / base_premium_usd, 2) as CoverageToPremiumRatio,
    
    ROUND(
        base_premium_usd / DATEDIFF(
            COALESCE(end_date, DATE_ADD(start_date, 365)), 
            start_date
        ), 
        2
    ) as PremiumPerDay,
    
    -- Premium tier categorization
    CASE 
        WHEN base_premium_usd < 1000 THEN 'Low'
        WHEN base_premium_usd >= 1000 AND base_premium_usd < 5000 THEN 'Medium'
        WHEN base_premium_usd >= 5000 AND base_premium_usd < 15000 THEN 'High'
        ELSE 'Premium'
    END as PremiumTier,
    
    -- Risk category based on category and coverage
    CASE 
        WHEN category = 'Motor' THEN
            CASE 
                WHEN coverage < 50000 THEN 'Low Risk'
                WHEN coverage >= 50000 AND coverage < 150000 THEN 'Medium Risk'
                ELSE 'High Risk'
            END
        WHEN category = 'Health' THEN
            CASE 
                WHEN coverage < 300000 THEN 'Medium Risk'
                WHEN coverage >= 300000 AND coverage < 800000 THEN 'High Risk'
                ELSE 'Very High Risk'
            END
        WHEN category = 'Life' THEN
            CASE 
                WHEN coverage < 100000 THEN 'Medium Risk'
                WHEN coverage >= 100000 AND coverage < 200000 THEN 'High Risk'
                ELSE 'Very High Risk'
            END
        WHEN category = 'Travel' THEN
            CASE 
                WHEN coverage < 5000 THEN 'Low Risk'
                ELSE 'Medium Risk'
            END
        WHEN category = 'Property' THEN
            CASE 
                WHEN coverage < 150000 THEN 'Medium Risk'
                WHEN coverage >= 150000 AND coverage < 300000 THEN 'High Risk'
                ELSE 'Very High Risk'
            END
        ELSE 'Unknown Risk'
    END as RiskCategory,
    
    
    -- Audit columns
    current_timestamp() as LoadDate
FROM bronze.insurance_policies
""",bronze_table=df_policies)

display(df_silver_policies)

# COMMAND ----------

loadIncrementalData(df_silver_policies,"silver","dimpolicies","PolicyHashKey")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from insureallbi.silver.dimpolicies limit 100

# COMMAND ----------

