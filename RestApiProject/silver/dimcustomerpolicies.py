# Databricks notebook source
# DBTITLE 1,Run Utilities
# MAGIC %run ../misc/Utilities

# COMMAND ----------

# DBTITLE 1,Set Catalog
# MAGIC %sql
# MAGIC USE CATALOG insureallBI

# COMMAND ----------

# DBTITLE 1,Set Configs
# ============================================
# Configuration - Update these values
# ============================================
SOURCE_TABLE = "insureallBI.bronze.customer_policies"  
TARGET_TABLE = "insureallBI.silver.dimcustomer_policies"

# COMMAND ----------

# DBTITLE 1,Read Bronze Table


# ============================================
# Read Bronze Data
# ============================================
df_customer_policies = spark.table(SOURCE_TABLE)
display(df_customer_policies)

# COMMAND ----------

# DBTITLE 1,Silver Transformation Logic with SQL
# ============================================
# Silver Transformation Logic using SQL
# ============================================


df_silver_customer_policies = spark.sql("""
SELECT 
    -- Hash Key (Surrogate Key) - Composite key for customer-policy relationship
    MD5(CONCAT(
        CAST(customer_id AS STRING), '|', 
        policy_id, '|',
        CAST(policy_enroll_date AS STRING)
    )) as CustomerPolicyHashKey,
    
    -- Original columns (standardized naming)
    customer_id as CustomerID,
    policy_id as PolicyID,
    policy_enroll_date as PolicyEnrollDate,
    
        
    -- Time-based dimensions for enrollment
    YEAR(policy_enroll_date) as EnrollmentYear,
    QUARTER(policy_enroll_date) as EnrollmentQuarter,
    MONTH(policy_enroll_date) as EnrollmentMonth,
    DATE_FORMAT(policy_enroll_date, 'MMMM') as EnrollmentMonthName,
    DAYOFWEEK(policy_enroll_date) as EnrollmentDayOfWeek,
    DATE_FORMAT(policy_enroll_date, 'EEEE') as EnrollmentDayName,
    WEEKOFYEAR(policy_enroll_date) as EnrollmentWeek,
    
    -- Policy age calculations
    DATEDIFF(current_date(), policy_enroll_date) as PolicyAgeDays,
    CAST(DATEDIFF(current_date(), policy_enroll_date) / 30.44 AS INT) as PolicyAgeMonths,
    CAST(DATEDIFF(current_date(), policy_enroll_date) / 365.25 AS INT) as PolicyAgeYears,
    
    -- Policy tenure category
    CASE 
        WHEN DATEDIFF(current_date(), policy_enroll_date) < 30 THEN 'New (< 1 month)'
        WHEN DATEDIFF(current_date(), policy_enroll_date) BETWEEN 30 AND 89 THEN 'Recent (1-3 months)'
        WHEN DATEDIFF(current_date(), policy_enroll_date) BETWEEN 90 AND 364 THEN 'Active (3-12 months)'
        WHEN DATEDIFF(current_date(), policy_enroll_date) BETWEEN 365 AND 1094 THEN 'Established (1-3 years)'
        WHEN DATEDIFF(current_date(), policy_enroll_date) BETWEEN 1095 AND 1824 THEN 'Long-term (3-5 years)'
        ELSE 'Veteran (5+ years)'
    END as PolicyTenureCategory,
    
    -- Is policy still within first year?
    CASE 
        WHEN DATEDIFF(current_date(), policy_enroll_date) <= 365 THEN TRUE
        ELSE FALSE
    END as IsFirstYear,
    
    -- Enrollment season
    CASE 
        WHEN MONTH(policy_enroll_date) IN (12, 1, 2) THEN 'Winter'
        WHEN MONTH(policy_enroll_date) IN (3, 4, 5) THEN 'Spring'
        WHEN MONTH(policy_enroll_date) IN (6, 7, 8) THEN 'Summer'
        ELSE 'Fall'
    END as EnrollmentSeason,
    
    -- Business period flags
    CASE 
        WHEN MONTH(policy_enroll_date) IN (1, 4, 7, 10) THEN 'Quarter Start'
        WHEN MONTH(policy_enroll_date) = 12 THEN 'Year End'
        ELSE 'Mid Period'
    END as BusinessPeriod,
    
    -- Enrollment timing flags
    CASE 
        WHEN DAY(policy_enroll_date) <= 7 THEN 'Early Month'
        WHEN DAY(policy_enroll_date) <= 15 THEN 'Mid Month'
        WHEN DAY(policy_enroll_date) <= 23 THEN 'Late Month'
        ELSE 'Month End'
    END as MonthPeriod,
    
    -- Is weekend enrollment?
    CASE 
        WHEN DAYOFWEEK(policy_enroll_date) IN (1, 7) THEN TRUE
        ELSE FALSE
    END as IsWeekendEnrollment,
    
    -- Calculate days to renewal (assuming annual policies)
    365 - (DATEDIFF(current_date(), policy_enroll_date) % 365) as DaysToNextRenewal,
    
    -- Renewal cycle (which year of the policy)
    CAST(DATEDIFF(current_date(), policy_enroll_date) / 365 AS INT) + 1 as RenewalCycleNumber,
    
    -- Is renewal approaching (within 60 days)?
    CASE 
        WHEN (365 - (DATEDIFF(current_date(), policy_enroll_date) % 365)) <= 60 THEN TRUE
        ELSE FALSE
    END as IsRenewalApproaching,
    
        
    -- Check if enrollment date is in the past
    CASE 
        WHEN policy_enroll_date > current_date() THEN FALSE
        ELSE TRUE
    END as IsHistoricalEnrollment,
    
    -- Audit columns
    current_timestamp() as LoadTimestamp,
    current_date() as LoadDate
    
FROM bronze.customer_policies
WHERE customer_id IS NOT NULL 
  AND policy_id IS NOT NULL 
  AND policy_enroll_date IS NOT NULL
""")

display(df_silver_customer_policies)

# COMMAND ----------

loadIncrementalData(df_silver_customer_policies,"silver","dimcustomer_policies","CustomerPolicyHashKey")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from insureallbi.silver.dimcustomer_policies limit  100