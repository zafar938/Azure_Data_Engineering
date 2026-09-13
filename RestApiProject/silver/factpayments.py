# Databricks notebook source
# DBTITLE 1,Run Utilities
# MAGIC %run ../misc/Utilities

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG insureallbi

# COMMAND ----------

# DBTITLE 1,Silver Transformation Logic with SQL
# ============================================
# Silver Transformation Logic - Fact Payments
# Using all bronze tables except claims
# Point-in-time joins for SCD Type 2 dimensions
# ============================================

df_fact_payments = spark.sql("""
WITH payments_base AS (
    SELECT 
        customer_id,
        policy_id,
        payment_date,
        payment_amount,
        payment_frequency,
        payment_mode,
        payment_status,
        transaction_id
    FROM insureallasqlcatalog.ins.payments
    UNION 
    SELECT 
        customer_id,
        policy_id,
        payment_date,
        payment_amount,
        payment_frequency,
        payment_mode,
        payment_status,
        transaction_id
    FROM bronze.insurance_payments
),
payments_enriched AS (
    SELECT 
        p.*,
        ROW_NUMBER() OVER (PARTITION BY p.transaction_id ORDER BY p.payment_date) as row_num
    FROM payments_base p
),
customers_enriched AS (
    SELECT 
        c.customer_id,
        c.name as customer_name,
        c.email,
        c.phone,
        c.gender,
        c.dob,
        c.occupation,
        c.address,
        c.city,
        c.state,
        c.country,
        c.pincode,
        c.Channel,
        c.is_active as customer_is_active,
        c.start_date as customer_start_date,
        c.end_date as customer_end_date,
        YEAR(CURRENT_DATE()) - YEAR(TO_DATE(replace(c.dob,'T00:00:00'), 'yyyy-MM-dd')) as customer_age
    FROM bronze.insurance_customers c
),
policies_enriched AS (
    SELECT 
        pol.policy_code as Policy_Id,
        pol.name as policy_name,
        pol.description as policy_description,
        pol.category as policy_category,
        pol.coverage_type,
        pol.base_premium_usd,
        CAST(pol.coverage AS DOUBLE) as policy_coverage,
        pol.currency,
        pol.term_period,
        pol.is_active as policy_is_active,
        pol.start_date as policy_start_date,
        pol.end_date as policy_end_date
    FROM bronze.insurance_policies pol
),
customer_policy_link AS (
    SELECT 
        cp.customer_id,
        cp.policy_id,
        cp.policy_enroll_date,
        ROW_NUMBER() OVER (PARTITION BY cp.customer_id, cp.policy_id ORDER BY cp.policy_enroll_date DESC) as latest_enrollment
    FROM bronze.customer_policies cp
)
SELECT 
    -- Surrogate Key (Fact Table Primary Key)
    MD5(CONCAT(
        COALESCE(pe.transaction_id, ''), '|',
        COALESCE(CAST(pe.payment_date AS STRING), ''), '|',
        COALESCE(CAST(pe.customer_id AS STRING), '')
    )) as PaymentHashKey,
    
    -- Foreign Keys
    pe.transaction_id as TransactionID,
    pe.customer_id as CustomerID,
    pe.policy_id as PolicyID,
    
    -- Payment Facts
    pe.payment_amount as PaymentAmount,
    pe.payment_date as PaymentDate,
    pe.payment_status as PaymentStatus,
    pe.payment_mode as PaymentMode,
    pe.payment_frequency as PaymentFrequency,
    
    -- Customer Dimension Attributes
    ce.customer_name as CustomerName,
    ce.email as CustomerEmail,
    ce.phone as CustomerPhone,
    ce.gender as CustomerGender,
    ce.customer_age as CustomerAge,
    ce.occupation as CustomerOccupation,
    ce.city as CustomerCity,
    ce.state as CustomerState,
    ce.country as CustomerCountry,
    ce.Channel as CustomerChannel,
    ce.customer_is_active as IsCustomerActive,
    
    -- Policy Dimension Attributes
    P.policy_name as PolicyName,
    P.policy_category as PolicyCategory,
    P.coverage_type as CoverageType,
    P.base_premium_usd as BasePremiumUSD,
    P.policy_coverage as PolicyCoverage,
    P.term_period as TermPeriod,
    P.policy_is_active as IsPolicyActive,
    
    -- Enrollment Information
    cpl.policy_enroll_date as PolicyEnrollDate,
    
    -- Time Dimensions
    YEAR(pe.payment_date) as PaymentYear,
    quarter(pe.payment_date) as PaymentQuarter,
    MONTH(pe.payment_date) as PaymentMonth,
    date_format(pe.payment_date, 'MMMM') as PaymentMonthName,
    DAYOFWEEK(pe.payment_date) as PaymentDayOfWeek,
    date_format(pe.payment_date, 'EEEE') as PaymentDayName,
    weekofyear(pe.payment_date) as PaymentWeekOfYear,
    
    -- Calculated Metrics
    CASE 
        WHEN pe.payment_status = 'Completed' THEN pe.payment_amount
        ELSE 0
    END as CompletedPaymentAmount,
    
    CASE 
        WHEN pe.payment_status = 'Failed' THEN pe.payment_amount
        ELSE 0
    END as FailedPaymentAmount,
    
    CASE 
        WHEN pe.payment_status = 'Pending' THEN pe.payment_amount
        ELSE 0
    END as PendingPaymentAmount,
    
    -- Payment to Premium Ratio
    CASE 
        WHEN p.base_premium_usd > 0 THEN 
            ROUND(pe.payment_amount / p.base_premium_usd, 4)
        ELSE NULL
    END as PaymentToPremiumRatio,
    
    -- Coverage Value Per Payment
    CASE 
        WHEN pe.payment_amount > 0 THEN 
            ROUND(p.policy_coverage / pe.payment_amount, 2)
        ELSE NULL
    END as CoveragePerPaymentDollar,
    
    -- Days Since Enrollment
    DATEDIFF(pe.payment_date, cpl.policy_enroll_date) as DaysSinceEnrollment,
    
    -- Payment Amount Tiers
    CASE 
        WHEN pe.payment_amount < 100 THEN 'Micro'
        WHEN pe.payment_amount >= 100 AND pe.payment_amount < 500 THEN 'Small'
        WHEN pe.payment_amount >= 500 AND pe.payment_amount < 2000 THEN 'Medium'
        WHEN pe.payment_amount >= 2000 AND pe.payment_amount < 10000 THEN 'Large'
        ELSE 'Premium'
    END as PaymentTier,
    
    -- Payment Status Flag
    CASE 
        WHEN pe.payment_status = 'Completed' THEN 1
        ELSE 0
    END as IsPaymentCompleted,
    
    CASE 
        WHEN pe.payment_status = 'Failed' THEN 1
        ELSE 0
    END as IsPaymentFailed,
    
    CASE 
        WHEN pe.payment_status = 'Pending' THEN 1
        ELSE 0
    END as IsPaymentPending,
    
    -- Risk Indicators
    CASE 
        WHEN pe.payment_status = 'Failed' AND pe.payment_mode = 'Credit Card' THEN 'High Risk'
        WHEN pe.payment_status = 'Pending' THEN 'Medium Risk'
        ELSE 'Low Risk'
    END as PaymentRisk,
    
    -- Audit Columns
    CURRENT_TIMESTAMP() as LoadDate
    
FROM payments_enriched pe
-- Point-in-time join for SCD Type 2 customer dimension
LEFT JOIN customers_enriched ce 
    ON pe.customer_id = ce.customer_id 
    AND pe.payment_date >= ce.customer_start_date 
    AND (pe.payment_date < ce.customer_end_date OR ce.customer_end_date IS NULL)
-- Point-in-time join for SCD Type 2 policy dimension
LEFT JOIN policies_enriched p 
    ON pe.policy_id = p.policy_id
    AND pe.payment_date >= p.policy_start_date 
    AND (pe.payment_date < p.policy_end_date OR p.policy_end_date IS NULL)
-- Join to customer_policies for enrollment date
LEFT JOIN customer_policy_link cpl 
    ON pe.customer_id = cpl.customer_id 
    AND pe.policy_id = cpl.policy_id 
    AND cpl.latest_enrollment = 1
WHERE pe.row_num = 1  -- Deduplication
""")

display(df_fact_payments)

# COMMAND ----------

loadIncrementalData(df_fact_payments,"silver","factpayments","PaymentHashKey")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from insureallbi.silver.factpayments limit 100

# COMMAND ----------

