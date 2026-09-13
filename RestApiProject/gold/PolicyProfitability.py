# Databricks notebook source
# MAGIC %run ../misc/Utilities

# COMMAND ----------

# DBTITLE 1,Policy Profitability - Overview
# MAGIC %md
# MAGIC # Policy Profitability - Gold Layer
# MAGIC
# MAGIC This notebook analyzes policy performance by calculating:
# MAGIC - Total revenue (premiums collected) by policy
# MAGIC - Total costs (claims paid) by policy
# MAGIC - Loss ratio (claims / premiums)
# MAGIC - Net profitability by policy type, category, and coverage type
# MAGIC - Active policies and enrollment trends
# MAGIC
# MAGIC **Source Tables:**
# MAGIC - insureallBI.silver.factpayments
# MAGIC - insureallBI.silver.factclaims
# MAGIC - insureallBI.bronze.insurance_policies
# MAGIC - insureallBI.bronze.customer_policies
# MAGIC
# MAGIC **Target Table:** insureallBI.gold.policy_profitability

# COMMAND ----------

# DBTITLE 1,Set Catalog
# MAGIC %sql
# MAGIC USE CATALOG insureallbi

# COMMAND ----------

# DBTITLE 1,Calculate Policy Profitability
# ============================================
# Gold Layer - Policy Profitability
# ============================================

df_policy_profitability = spark.sql("""
WITH policy_revenue AS (
    SELECT 
        p.PolicyID,
        p.PolicyName,
        p.PolicyCategory,
        p.CoverageType,
        COUNT(DISTINCT p.CustomerID) as TotalCustomers,
        COUNT(DISTINCT p.TransactionID) as TotalPayments,
        SUM(p.PaymentAmount) as TotalRevenue,
        MIN(p.PaymentDate) as FirstPaymentDate,
        MAX(p.PaymentDate) as LastPaymentDate
    FROM silver.factpayments p
    WHERE p.PaymentStatus = 'Success'
    GROUP BY p.PolicyID, p.PolicyName, p.PolicyCategory, p.CoverageType
),
policy_claims AS (
    SELECT 
        c.PolicyID,
        COUNT(DISTINCT c.ClaimID) as TotalClaims,
        COUNT(DISTINCT c.CustomerID) as CustomersWithClaims,
        SUM(c.ClaimAmount) as TotalClaimAmount,
        SUM(c.SettlementAmount) as TotalSettlementAmount,
        SUM(CASE WHEN c.IsFraudulent = TRUE THEN 1 ELSE 0 END) as FraudClaimCount,
        MIN(c.ClaimDate) as FirstClaimDate,
        MAX(c.ClaimDate) as LastClaimDate
    FROM silver.factclaims c
    GROUP BY c.PolicyID
),
policy_enrollments AS (
    SELECT 
        policy_id,
        COUNT(*) as TotalEnrollments,
        COUNT(DISTINCT customer_id) as UniqueCustomers,
        MIN(policy_enroll_date) as FirstEnrollmentDate,
        MAX(policy_enroll_date) as LastEnrollmentDate
    FROM bronze.customer_policies
    GROUP BY policy_id
)
SELECT 
    -- Policy Identifiers
    pr.PolicyID,
    pr.PolicyName,
    pr.PolicyCategory,
    pr.CoverageType,
    p.BasePremiumUSD as BasePremium,
    p.coverage as CoverageAmount,
    
    -- Customer Metrics
    COALESCE(pe.TotalEnrollments, 0) as TotalEnrollments,
    COALESCE(pe.UniqueCustomers, 0) as UniqueCustomersEnrolled,
    COALESCE(pr.TotalCustomers, 0) as PayingCustomers,
    COALESCE(pc.CustomersWithClaims, 0) as CustomersWithClaims,
    
    -- Revenue Metrics
    COALESCE(pr.TotalPayments, 0) as TotalPayments,
    COALESCE(pr.TotalRevenue, 0) as TotalRevenue,
    pr.FirstPaymentDate,
    pr.LastPaymentDate,
    
    -- Claims Metrics
    COALESCE(pc.TotalClaims, 0) as TotalClaims,
    COALESCE(pc.TotalClaimAmount, 0) as TotalClaimAmount,
    COALESCE(pc.TotalSettlementAmount, 0) as TotalSettlementPaid,
    COALESCE(pc.FraudClaimCount, 0) as FraudClaimCount,
    pc.FirstClaimDate,
    pc.LastClaimDate,
    
    -- Profitability Metrics
    COALESCE(pr.TotalRevenue, 0) - COALESCE(pc.TotalSettlementAmount, 0) as NetProfit,
    
    CASE 
        WHEN pr.TotalRevenue > 0 
        THEN ROUND(COALESCE(pc.TotalSettlementAmount, 0) / pr.TotalRevenue * 100, 2)
        ELSE 0
    END as LossRatio,
    
    CASE 
        WHEN pr.TotalCustomers > 0
        THEN ROUND(COALESCE(pc.TotalClaims, 0) / CAST(pr.TotalCustomers AS DOUBLE), 2)
        ELSE 0
    END as ClaimsPerCustomer,
    
    CASE 
        WHEN pr.TotalCustomers > 0
        THEN ROUND(pr.TotalRevenue / pr.TotalCustomers, 2)
        ELSE 0
    END as RevenuePerCustomer,
    
    CASE 
        WHEN pe.UniqueCustomers > 0
        THEN ROUND(COALESCE(pc.CustomersWithClaims, 0) / CAST(pe.UniqueCustomers AS DOUBLE) * 100, 2)
        ELSE 0
    END as ClaimFrequencyPercent,
    
    -- Performance Categorization
    CASE 
        WHEN COALESCE(pr.TotalRevenue, 0) - COALESCE(pc.TotalSettlementAmount, 0) > 100000 THEN 'Highly Profitable'
        WHEN COALESCE(pr.TotalRevenue, 0) - COALESCE(pc.TotalSettlementAmount, 0) > 50000 THEN 'Profitable'
        WHEN COALESCE(pr.TotalRevenue, 0) - COALESCE(pc.TotalSettlementAmount, 0) > 0 THEN 'Marginally Profitable'
        ELSE 'Loss Making'
    END as ProfitabilityCategory,
    
    CASE 
        WHEN COALESCE(pc.TotalSettlementAmount, 0) / NULLIF(pr.TotalRevenue, 0) > 1.0 THEN 'Critical - Loss Ratio > 100%'
        WHEN COALESCE(pc.TotalSettlementAmount, 0) / NULLIF(pr.TotalRevenue, 0) > 0.8 THEN 'High Risk - Loss Ratio 80-100%'
        WHEN COALESCE(pc.TotalSettlementAmount, 0) / NULLIF(pr.TotalRevenue, 0) > 0.6 THEN 'Medium Risk - Loss Ratio 60-80%'
        ELSE 'Low Risk - Loss Ratio < 60%'
    END as RiskLevel,
    
    -- Audit
    current_timestamp() as LoadDate
    
FROM policy_revenue pr
LEFT JOIN policy_claims pc ON pr.PolicyID = pc.PolicyID
LEFT JOIN policy_enrollments pe ON pr.PolicyID = pe.policy_id
LEFT JOIN silver.dimpolicies p ON pr.PolicyID = p.PolicyCode
""")

display(df_policy_profitability)

# COMMAND ----------

# DBTITLE 1,Write to Gold Table
writeDfToTable(df_policy_profitability, "gold", "policy_profitability")