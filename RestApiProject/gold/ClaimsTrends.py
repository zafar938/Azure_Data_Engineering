# Databricks notebook source
# MAGIC %run ../misc/Utilities

# COMMAND ----------

# DBTITLE 1,Claims Trends - Overview
# MAGIC %md
# MAGIC # Claims Trends Analysis - Gold Layer
# MAGIC
# MAGIC This notebook provides time-series analysis of claims:
# MAGIC - Daily, monthly, quarterly, and yearly claim aggregations
# MAGIC - Claim volume and amount trends
# MAGIC - Settlement ratios and approval rates
# MAGIC - Fraud claim trends
# MAGIC - Claims by type (Hospitalization, Theft, Death, Accident, Damage)
# MAGIC - Average processing times
# MAGIC
# MAGIC **Source Tables:**
# MAGIC - insureallBI.silver.factclaims
# MAGIC
# MAGIC **Target Table:** insureallBI.gold.claims_trends

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG insureallbi

# COMMAND ----------

# DBTITLE 1,Calculate Claims Trends
# ============================================
# Gold Layer - Claims Trends
# ============================================

df_claims_trends = spark.sql("""
SELECT 
    -- Time Dimensions
    ClaimYear,
    ClaimQuarter,
    ClaimMonth,
    ClaimMonthName,
    DATE_TRUNC('month', ClaimDate) as ClaimMonthDate,
    
    -- Claim Type Breakdown
    ClaimType,
    ClaimStatus,
    ClaimSizeCategory,
    
    
    -- Channel
    Channel,
    
    -- Aggregated Metrics
    COUNT(DISTINCT ClaimID) as TotalClaims,
    COUNT(DISTINCT CustomerID) as UniqueCustomers,
    COUNT(DISTINCT PolicyID) as UniquePolicies,
    
    -- Financial Metrics
    SUM(ClaimAmount) as TotalClaimAmount,
    SUM(SettlementAmount) as TotalSettlementAmount,
    AVG(ClaimAmount) as AvgClaimAmount,
    AVG(SettlementAmount) as AvgSettlementAmount,
    MIN(ClaimAmount) as MinClaimAmount,
    MAX(ClaimAmount) as MaxClaimAmount,
    
    -- Settlement Metrics
    ROUND(AVG(SettlementRatio), 2) as AvgSettlementRatio,
    
    COUNT(CASE WHEN ClaimStatus = 'Approved' THEN 1 END) as ApprovedClaims,
    COUNT(CASE WHEN ClaimStatus = 'Rejected' THEN 1 END) as RejectedClaims,
    COUNT(CASE WHEN ClaimStatus = 'Pending' THEN 1 END) as PendingClaims,
    
    ROUND(
        COUNT(CASE WHEN ClaimStatus = 'Approved' THEN 1 END) / 
        CAST(COUNT(*) AS DOUBLE) * 100, 2
    ) as ApprovalRate,
    
    -- Processing Time Metrics
    AVG(ProcessingDays) as AvgProcessingDays,
    AVG(ReportedDelayDays) as AvgReportedDelayDays,
    AVG(IncidentToClaimDays) as AvgIncidentToClaimDays,
    
    -- Fraud Metrics
    SUM(CASE WHEN IsFraudulent = TRUE THEN 1 ELSE 0 END) as FraudClaimCount,
    ROUND(
        SUM(CASE WHEN IsFraudulent = TRUE THEN 1 ELSE 0 END) / 
        CAST(COUNT(*) AS DOUBLE) * 100, 2
    ) as FraudClaimPercent,
    
    SUM(CASE WHEN IsFraudulent = TRUE THEN ClaimAmount ELSE 0 END) as FraudClaimAmount,
    
    -- Risk Distribution
    COUNT(CASE WHEN RiskIndicator LIKE 'High Risk%' THEN 1 END) as HighRiskClaims,
    COUNT(CASE WHEN RiskIndicator LIKE 'Medium Risk%' THEN 1 END) as MediumRiskClaims,
    COUNT(CASE WHEN RiskIndicator = 'Normal' THEN 1 END) as NormalRiskClaims,
    
    -- Audit
    current_timestamp() as LoadDate
    
FROM silver.factclaims
GROUP BY 
    ClaimYear,
    ClaimQuarter,
    ClaimMonth,
    ClaimMonthName,
    DATE_TRUNC('month', ClaimDate),
    ClaimType,
    ClaimStatus,
    ClaimSizeCategory,
    Channel
ORDER BY ClaimYear DESC, ClaimMonth DESC, ClaimType
""")

display(df_claims_trends)

# COMMAND ----------

# DBTITLE 1,Write to Gold Table
writeDfToTable(df_claims_trends, "gold", "claimstrends")