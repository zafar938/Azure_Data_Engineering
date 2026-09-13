# Databricks notebook source
# MAGIC %run ../misc/Utilities

# COMMAND ----------

# DBTITLE 1,Customer Lifetime Value - Overview
# MAGIC %md
# MAGIC # Customer Lifetime Value (CLV) - Gold Layer
# MAGIC
# MAGIC This notebook aggregates customer-level metrics to calculate lifetime value:
# MAGIC - Total premiums paid per customer
# MAGIC - Total claims filed and settled amounts
# MAGIC - Net profitability (premiums - claims)
# MAGIC - Customer tenure and activity status
# MAGIC - Risk indicators
# MAGIC
# MAGIC **Source Tables:**
# MAGIC - insureallBI.silver.factpayments
# MAGIC - insureallBI.silver.factclaims
# MAGIC - insureallBI.bronze.insurance_customers
# MAGIC
# MAGIC **Target Table:** insureallBI.gold.customer_lifetime_value

# COMMAND ----------

# DBTITLE 1,Use Catalog
# MAGIC %sql
# MAGIC USE CATALOG insureallbi

# COMMAND ----------

# DBTITLE 1,Calculate Customer Lifetime Value
# ============================================
# Gold Layer - Customer Lifetime Value
# ============================================

df_customer_lifetime_value = spark.sql("""
WITH customer_payments AS (
    SELECT 
        CustomerID,
        COUNT(DISTINCT TransactionID) as TotalPayments,
        SUM(PaymentAmount) as TotalPremiumsPaid,
        MIN(PaymentDate) as FirstPaymentDate,
        MAX(PaymentDate) as LastPaymentDate,
        COUNT(DISTINCT PolicyID) as UniquePoliciesTotalPaid
    FROM insureallBI.silver.factpayments
    WHERE PaymentStatus = 'Success'
    GROUP BY CustomerID
),
customer_claims AS (
    SELECT 
        CustomerID,
        COUNT(DISTINCT ClaimID) as TotalClaims,
        SUM(ClaimAmount) as TotalClaimAmount,
        SUM(SettlementAmount) as TotalSettlementAmount,
        SUM(CASE WHEN IsFraudulent = TRUE THEN 1 ELSE 0 END) as FraudClaimCount,
        MIN(ClaimDate) as FirstClaimDate,
        MAX(ClaimDate) as LastClaimDate
    FROM insureallBI.silver.factclaims
    GROUP BY CustomerID
)
SELECT 
    -- Customer ID
    COALESCE(cp.CustomerID, cc.CustomerID) as CustomerID,
    
    -- Customer Demographics
    c.gender as Gender,
    c.age as Age,
    c.country as Country,
    
    -- Payment Metrics
    COALESCE(cp.TotalPayments, 0) as TotalPayments,
    COALESCE(cp.TotalPremiumsPaid, 0) as TotalPremiumsPaid,
    COALESCE(cp.UniquePoliciesTotalPaid, 0) as UniquePoliciesPaid,
    cp.FirstPaymentDate,
    cp.LastPaymentDate,
    DATEDIFF(COALESCE(cp.LastPaymentDate, current_date()), cp.FirstPaymentDate) as CustomerTenureDays,
    
    -- Claims Metrics
    COALESCE(cc.TotalClaims, 0) as TotalClaims,
    COALESCE(cc.TotalClaimAmount, 0) as TotalClaimAmount,
    COALESCE(cc.TotalSettlementAmount, 0) as TotalSettlementAmount,
    COALESCE(cc.FraudClaimCount, 0) as FraudClaimCount,
    cc.FirstClaimDate,
    cc.LastClaimDate,
    
    -- Calculated Metrics
    COALESCE(cp.TotalPremiumsPaid, 0) - COALESCE(cc.TotalSettlementAmount, 0) as NetProfitability,
    
    CASE 
        WHEN cp.TotalPremiumsPaid > 0 
        THEN ROUND(COALESCE(cc.TotalSettlementAmount, 0) / cp.TotalPremiumsPaid * 100, 2)
        ELSE 0
    END as ClaimToPremiumRatio,
    
    CASE 
        WHEN cp.TotalPayments > 0 
        THEN ROUND(COALESCE(cc.TotalClaims, 0) / CAST(cp.TotalPayments AS DOUBLE), 2)
        ELSE 0
    END as ClaimFrequency,
    
    CASE 
        WHEN cp.TotalPremiumsPaid > 0
        THEN ROUND(cp.TotalPremiumsPaid / NULLIF(DATEDIFF(COALESCE(cp.LastPaymentDate, current_date()), cp.FirstPaymentDate), 0) * 365, 2)
        ELSE 0
    END as AnnualPremiumValue,
    
    -- Customer Segmentation
    CASE 
        WHEN cp.TotalPremiumsPaid - COALESCE(cc.TotalSettlementAmount, 0) > 50000 THEN 'High Value'
        WHEN cp.TotalPremiumsPaid - COALESCE(cc.TotalSettlementAmount, 0) > 20000 THEN 'Medium Value'
        WHEN cp.TotalPremiumsPaid - COALESCE(cc.TotalSettlementAmount, 0) > 0 THEN 'Low Value'
        ELSE 'Loss Making'
    END as CustomerValueSegment,
    
    CASE 
        WHEN COALESCE(cc.FraudClaimCount, 0) > 0 THEN 'High Risk - Fraud'
        WHEN COALESCE(cc.TotalClaims, 0) > 5 THEN 'High Risk - Frequent Claims'
        WHEN COALESCE(cc.TotalSettlementAmount, 0) / NULLIF(cp.TotalPremiumsPaid, 0) > 0.8 THEN 'Medium Risk'
        ELSE 'Low Risk'
    END as RiskProfile,
    
    CASE 
        WHEN DATEDIFF(current_date(), cp.LastPaymentDate) <= 90 THEN 'Active'
        WHEN DATEDIFF(current_date(), cp.LastPaymentDate) <= 180 THEN 'At Risk'
        ELSE 'Inactive'
    END as CustomerStatus,
    
    -- Audit
    current_timestamp() as LoadDate
    
FROM customer_payments cp
FULL OUTER JOIN customer_claims cc ON cp.CustomerID = cc.CustomerID
LEFT JOIN silver.dimcustomer c ON COALESCE(cp.CustomerID, cc.CustomerID) = c.CustomerID
""")

display(df_customer_lifetime_value)

# COMMAND ----------

# DBTITLE 1,Write to Gold Table
writeDfToTable(df_customer_lifetime_value, "gold", "customerlifetimevalue")