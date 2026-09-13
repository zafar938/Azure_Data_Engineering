# Databricks notebook source
# DBTITLE 1,Run Utilities
# MAGIC %run ../misc/Utilities

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG insureallbi

# COMMAND ----------

# DBTITLE 1,Silver Transformation Logic with SQL
# ============================================
# Silver Transformation Logic - Fact Claims
# ============================================


df_silver_factclaims = spark.sql("""
WITH ranked_enrollments AS (
    SELECT 
        customer_id,
        policy_id,
        policy_enroll_date,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id, policy_id 
            ORDER BY policy_enroll_date DESC
        ) as rn
    FROM bronze.customer_policies
)
SELECT 
    -- Surrogate Key (Hash Key)
    MD5(CONCAT(c.claim_id, '|', CAST(c.incident_date AS STRING), '|',CAST(c.policy_id AS STRING) )) as ClaimHashKey,
    
    -- Business Keys
    c.claim_id as ClaimID,
    cp.policy_id as PolicyID,
    c.customer_id as CustomerID,
    co.country_id as CountryID,
    
    -- Claim Details
    c.claim_type as ClaimType,
    c.claim_status as ClaimStatus,
    c.channel as Channel,
    CASE WHEN c.fraud_flag = '1' THEN TRUE ELSE FALSE END as IsFraudulent,
    
    -- Financial Metrics
    c.claim_amount as ClaimAmount,
    CAST(c.settlement_amount as DOUBLE) as SettlementAmount,
    p.coverage as PolicyCoverage,
    p.base_premium_usd as PolicyPremium,
    

    --RiskAnalysis
    CASE 
        WHEN c.Settlement_Amount > c.claim_amount THEN 1
        ELSE 0
    END InternalRiskRating,

    CASE InternalRiskRating
        WHEN 1 THEN c.Settlement_Amount > c.claim_amount
        ELSE NULL
    END OverPaidAmount, 

    -- Calculated Financial Metrics
    ROUND(
        CAST(c.settlement_amount as DOUBLE) / NULLIF(c.claim_amount, 0) * 100, 
        2
    ) as SettlementRatio,
    
    ROUND(
        c.claim_amount / NULLIF(p.coverage, 0) * 100, 
        2
    ) as ClaimToCoverageRatio,
    
    ROUND(
        c.claim_amount / NULLIF(p.base_premium_usd, 0), 
        2
    ) as ClaimToPremiumRatio,
    
    CASE 
        WHEN c.claim_amount < 5000 THEN 'Small'
        WHEN c.claim_amount >= 5000 AND c.claim_amount < 20000 THEN 'Medium'
        WHEN c.claim_amount >= 20000 AND c.claim_amount < 50000 THEN 'Large'
        ELSE 'Very Large'
    END as ClaimSizeCategory,
    
    -- Temporal Attributes
    c.incident_date as IncidentDate,
    c.claim_date as ClaimDate,
    TO_DATE(replace(c.approval_date,'T00:00:00',''), 'yyyy-MM-dd') as ApprovalDate,
    
    YEAR(c.claim_date) as ClaimYear,
    QUARTER(c.claim_date) as ClaimQuarter,
    MONTH(c.claim_date) as ClaimMonth,
    DATE_FORMAT(c.claim_date, 'MMMM') as ClaimMonthName,
    DAYOFWEEK(c.claim_date) as ClaimDayOfWeek,
    DATE_FORMAT(c.claim_date, 'EEEE') as ClaimDayName,
    
    -- Calculated Time Metrics
    c.reported_delay_days as ReportedDelayDays,
    
    DATEDIFF(
        TO_DATE(replace(c.approval_date,'T00:00:00','')), 
        c.claim_date
    ) as ProcessingDays,
    
    DATEDIFF(
        c.claim_date, 
        c.incident_date
    ) as IncidentToClaimDays,
    
    CASE 
        WHEN c.reported_delay_days <= 1 THEN 'Immediate'
        WHEN c.reported_delay_days <= 7 THEN 'Within Week'
        WHEN c.reported_delay_days <= 30 THEN 'Within Month'
        ELSE 'Delayed'
    END as ReportingTimeCategory,
    
    -- Policy Context
    p.name as PolicyName,
    p.category as PolicyCategory,
    p.coverage_type as CoverageType,
    
    -- Customer Context
    cust.name as CustomerName,
    cust.email as CustomerEmail,
    cust.phone as CustomerPhone,
    cust.address as CustomerAddress,
    cust.city as CustomerCity,
    cust.state as CustomerState,
    cust.country as CustomerCountry,
    cust.pincode as CustomerPincode,
    cust.gender as CustomerGender,
         
    -- Risk Indicators
    CASE 
        WHEN c.fraud_flag = '1' THEN 'High Risk - Fraud'
        WHEN c.claim_amount > p.coverage THEN 'High Risk - Over Coverage'
        WHEN c.reported_delay_days > 30 THEN 'Medium Risk - Late Report'
        WHEN CAST(c.settlement_amount as DOUBLE) / NULLIF(c.claim_amount, 0) < 0.5 THEN 'Medium Risk - Low Settlement'
        ELSE 'Normal'
    END as RiskIndicator,
    
    -- Audit columns
    current_timestamp() as LoadDate
    
FROM bronze.insurance_claims c
LEFT JOIN ranked_enrollments cp 
    ON c.customer_id = cp.customer_id 
    AND c.policy_id = cp.policy_id
    AND cp.policy_enroll_date <= c.claim_date
    AND cp.rn = 1
LEFT JOIN bronze.insurance_policies p 
    ON c.policy_id = p.policy_code AND c.claim_date BETWEEN p.start_date AND p.end_date
LEFT JOIN bronze.insurance_customers cust 
    ON c.customer_id = cust.customer_id and cust.end_date is null
LEFT JOIN bronze.insurance_countries co 
    ON cust.country = co.country_name 
""")

display(df_silver_factclaims)

# COMMAND ----------

# DBTITLE 1,Load to Silver Table
loadIncrementalData(df_silver_factclaims, "silver", "factclaims", "ClaimHashKey")

# COMMAND ----------

# Test case

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.insurance_customers where customer_id = 65

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Fixed query: picks the most recent policy enrollment before or on the claim date
# MAGIC WITH ranked_enrollments AS (
# MAGIC     SELECT 
# MAGIC         customer_id,
# MAGIC         policy_id,
# MAGIC         policy_enroll_date,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY customer_id, policy_id 
# MAGIC             ORDER BY policy_enroll_date DESC
# MAGIC         ) as rn
# MAGIC     FROM bronze.customer_policies
# MAGIC )
# MAGIC SELECT * 
# MAGIC     -- c.*,
# MAGIC     -- cp.policy_enroll_date
# MAGIC FROM bronze.insurance_claims c
# MAGIC LEFT JOIN ranked_enrollments cp  
# MAGIC     ON c.customer_id = cp.customer_id 
# MAGIC     AND c.policy_id = cp.policy_id
# MAGIC     AND cp.policy_enroll_date <= c.claim_date
# MAGIC     AND cp.rn = 1
# MAGIC LEFT JOIN bronze.insurance_policies p 
# MAGIC     ON c.policy_id = p.policy_code 
# MAGIC AND c.claim_date BETWEEN p.start_date AND p.end_date
# MAGIC -- LEFT JOIN bronze.insurance_customers cust 
# MAGIC --     ON c.customer_id = cust.customer_id
# MAGIC WHERE c.claim_id='CLM405701691' 
# MAGIC -- and cust.end_date is null

# COMMAND ----------

