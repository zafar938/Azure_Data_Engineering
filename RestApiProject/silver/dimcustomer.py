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
SOURCE_TABLE = "insureallBI.bronze.insurance_customers"  
TARGET_TABLE = "insureallBI.silver.dimcustomer"

# COMMAND ----------

# DBTITLE 1,Read Bronze Table


# ============================================
# Read Bronze Data
# ============================================
df_customer = spark.table(SOURCE_TABLE)
display(df_customer)

# COMMAND ----------

# DBTITLE 1,Silver Transformation Logic with SQL
# ============================================
# Silver Transformation Logic using SQL
# ============================================

df_silver_customers = spark.sql("""
SELECT 
    -- Hash Key (Surrogate Key)
    MD5(CONCAT(CAST(customer_id AS STRING), '|', CAST(start_date AS STRING))) as CustomerHashKey,
    
    -- Original columns with renamed/cleaned format
    customer_id as CustomerID,
    name as CustomerName,
    email as Email,
    phone as Phone,
    gender as Gender,
    occupation as Occupation,
    Channel as AcquisitionChannel,
    
    -- Address fields
    address as Address,
    city as City,
    state as State,
    country as Country,
    pincode as Pincode,
    
    -- Nominee information
    CASE WHEN UPPER(nominated) = 'YES' THEN TRUE ELSE FALSE END as HasNominee,
    nominated as NominatedFlag,
    nominee_relation as NomineeRelation,
    
    -- Date fields with proper conversion
    TO_DATE(REPLACE(dob,'T00:00:00',''), 'yyyy-MM-dd') as DateOfBirth,
    TO_DATE(REPLACE(datesignedup,'T00:00:00',''), 'yyyy-MM-dd') as DateSignedUp,
    start_date as StartDate,
    end_date as EndDate,
    is_active as IsActive,
    
    -- Age calculation
    CAST(DATEDIFF(current_date(), TO_DATE(REPLACE(dob,'T00:00:00',''), 'yyyy-MM-dd')) / 365.25 AS INT) as Age,
    
    -- Age group categorization
    CASE 
        WHEN CAST(DATEDIFF(current_date(), TO_DATE(REPLACE(dob,'T00:00:00',''), 'yyyy-MM-dd')) / 365.25 AS INT) < 18 THEN 'Minor (<18)'
        WHEN CAST(DATEDIFF(current_date(), TO_DATE(REPLACE(dob,'T00:00:00',''), 'yyyy-MM-dd')) / 365.25 AS INT) BETWEEN 18 AND 25 THEN 'Young Adult (18-25)'
        WHEN CAST(DATEDIFF(current_date(), TO_DATE(REPLACE(dob,'T00:00:00',''), 'yyyy-MM-dd')) / 365.25 AS INT) BETWEEN 26 AND 35 THEN 'Adult (26-35)'
        WHEN CAST(DATEDIFF(current_date(), TO_DATE(REPLACE(dob,'T00:00:00',''), 'yyyy-MM-dd')) / 365.25 AS INT) BETWEEN 36 AND 45 THEN 'Middle Age (36-45)'
        WHEN CAST(DATEDIFF(current_date(), TO_DATE(REPLACE(dob,'T00:00:00',''), 'yyyy-MM-dd')) / 365.25 AS INT) BETWEEN 46 AND 60 THEN 'Senior (46-60)'
        ELSE 'Elderly (60+)'
    END as AgeGroup,
    
    -- Customer tenure calculation
    DATEDIFF(
        COALESCE(end_date, current_date()), 
        start_date
    ) as CustomerTenureDays,
    
    -- Tenure category
    CASE 
        WHEN DATEDIFF(COALESCE(end_date, current_date()), start_date) < 365 THEN 'New (< 1 year)'
        WHEN DATEDIFF(COALESCE(end_date, current_date()), start_date) BETWEEN 365 AND 1095 THEN 'Regular (1-3 years)'
        WHEN DATEDIFF(COALESCE(end_date, current_date()), start_date) BETWEEN 1096 AND 1825 THEN 'Long-term (3-5 years)'
        ELSE 'Loyal (5+ years)'
    END as TenureCategory,
    
    -- Email domain extraction
    SUBSTRING_INDEX(email, '@', -1) as EmailDomain,
    
    -- Occupation category
    CASE 
        WHEN occupation IN ('Doctor', 'Nurse') THEN 'Healthcare'
        WHEN occupation IN ('Engineer', 'Analyst', 'Developer') THEN 'IT/Engineering'
        WHEN occupation IN ('Teacher', 'Professor') THEN 'Education'
        WHEN occupation IN ('Clerk', 'Accountant', 'Manager') THEN 'Business/Administration'
        WHEN occupation IN ('Lawyer', 'Judge') THEN 'Legal'
        ELSE 'Other'
    END as OccupationCategory,
    
    -- Channel standardization
    UPPER(TRIM(Channel)) as ChannelStandardized,
    
    -- Signup year for time-based analysis
    YEAR(start_date) as SignupYear,
    QUARTER(start_date) as SignupQuarter,
    MONTH(start_date) as SignupMonth,
    DATE_FORMAT(start_date, 'MMMM') as SignupMonthName,
    
    -- Data quality flags
    CASE 
        WHEN email IS NULL OR email = '' THEN FALSE
        WHEN email LIKE '%@%' THEN TRUE
        ELSE FALSE
    END as HasValidEmail,
    
    CASE 
        WHEN phone IS NULL OR phone = '' THEN FALSE
        WHEN LENGTH(phone) >= 10 THEN TRUE
        ELSE FALSE
    END as HasValidPhone,
    
    -- Geographic region mapping (example for India)
    CASE 
        WHEN state IN ('Maharashtra', 'Gujarat', 'Goa') THEN 'West'
        WHEN state IN ('Tamil Nadu', 'Karnataka', 'Kerala', 'Andhra Pradesh', 'Telangana') THEN 'South'
        WHEN state IN ('Uttar Pradesh', 'Delhi', 'Haryana', 'Punjab', 'Rajasthan', 'Uttarakhand') THEN 'North'
        WHEN state IN ('West Bengal', 'Bihar', 'Jharkhand', 'Odisha', 'Sikkim') THEN 'East'
        WHEN state IN ('Madhya Pradesh', 'Chhattisgarh') THEN 'Central'
        ELSE 'Other'
    END as Region,
    
    -- Audit columns
    current_timestamp() as LoadTimestamp
    
FROM bronze.insurance_customers
""")

display(df_silver_customers)

# COMMAND ----------

loadIncrementalData(df_silver_customers,"silver","dimcustomer","CustomerHashKey")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from insureallbi.silver.dimcustomer limit 100