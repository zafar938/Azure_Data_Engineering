# Databricks notebook source
# DBTITLE 1,Run Utilities
# MAGIC %run ../misc/Utilities

# COMMAND ----------

# DBTITLE 1,Business Health Metrics - Overview
# MAGIC %md
# MAGIC # Business Health Metrics - Gold Layer
# MAGIC
# MAGIC This notebook calculates key operational KPIs for business health monitoring:
# MAGIC - Combined Loss Ratio (industry standard KPI)
# MAGIC - Customer Acquisition Cost (CAC)
# MAGIC - Customer Retention Rate
# MAGIC - Channel Performance (Online, Agent, Hospital, Garage)
# MAGIC - Product Mix Distribution
# MAGIC - Payment Success Rates
# MAGIC - Fraud Detection Efficiency
# MAGIC - Claims Approval Rates
# MAGIC
# MAGIC **Source Tables:**
# MAGIC - insureallBI.silver.factpayments
# MAGIC - insureallBI.silver.factclaims
# MAGIC - insureallBI.bronze.customer_policies
# MAGIC - insureallBI.bronze.insurance_customers
# MAGIC
# MAGIC **Target Table:** insureallBI.gold.business_health_metrics

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG insureallbi

# COMMAND ----------

# DBTITLE 1,Calculate Business Health Metrics
# ============================================
# Gold Layer - Business Health Metrics
# ============================================

df_business_health = spark.sql("""
WITH overall_metrics AS (
    SELECT     'Overall Business' as MetricScope,
    COUNT(DISTINCT c.ClaimID) as TotalClaims,
    SUM(c.ClaimAmount) as TotalClaimAmount,
    SUM(c.SettlementAmount) as TotalSettlementPaid,
    COUNT(CASE WHEN c.ClaimStatus = 'Approved' THEN 1 END) as ApprovedClaims,
    COUNT(CASE WHEN c.IsFraudulent = TRUE THEN 1 END) as FraudClaims
    
        
    FROM silver.factclaims c
)
SELECT 
    om.MetricScope,
    om.TotalClaims,
    om.TotalClaimAmount,
    om.TotalSettlementPaid,
    om.ApprovedClaims,
    om.FraudClaims,
    
    ROUND(
        try_divide(om.ApprovedClaims, CAST(om.TotalClaims AS DOUBLE)) * 100, 2
    ) as ClaimApprovalRate,
    
    ROUND(
        try_divide(om.FraudClaims, CAST(om.TotalClaims AS DOUBLE)) * 100, 2
    ) as FraudDetectionRate,
       
    -- Audit
    current_timestamp() as LoadDate
    
FROM overall_metrics om
""")

display(df_business_health.limit(100))

# COMMAND ----------

writeDfToTable(df_business_health,"gold","businesshealth")

# COMMAND ----------

# DBTITLE 1,Calculate Channel Performance
# ============================================
# Channel Performance Breakdown
# ============================================

df_channel_performance = spark.sql("""
SELECT 
    Channel,
    
    -- Claims by Channel
    COUNT(DISTINCT ClaimID) as TotalClaims,
    SUM(ClaimAmount) as TotalClaimAmount,
    SUM(SettlementAmount) as TotalSettlementPaid,
    AVG(ProcessingDays) as AvgProcessingDays,
    
    -- Claim Status Distribution
    COUNT(CASE WHEN ClaimStatus = 'Approved' THEN 1 END) as ApprovedClaims,
    COUNT(CASE WHEN ClaimStatus = 'Rejected' THEN 1 END) as RejectedClaims,
    COUNT(CASE WHEN ClaimStatus = 'Pending' THEN 1 END) as PendingClaims,
    
    ROUND(
        COUNT(CASE WHEN ClaimStatus = 'Approved' THEN 1 END) / 
        CAST(COUNT(*) AS DOUBLE) * 100, 2
    ) as ApprovalRate,
    
    -- Fraud Detection
    COUNT(CASE WHEN IsFraudulent = TRUE THEN 1 END) as FraudClaims,
    
    ROUND(
        COUNT(CASE WHEN IsFraudulent = TRUE THEN 1 END) / 
        CAST(COUNT(*) AS DOUBLE) * 100, 2
    ) as FraudRate,
    
    -- Performance Rating
    CASE 
        WHEN AVG(ProcessingDays) < 5 THEN 'Excellent'
        WHEN AVG(ProcessingDays) < 10 THEN 'Good'
        WHEN AVG(ProcessingDays) < 15 THEN 'Average'
        ELSE 'Needs Improvement'
    END as EfficiencyRating,
    
    current_timestamp() as LoadDate
    
FROM insureallBI.silver.factclaims
GROUP BY Channel
ORDER BY TotalClaims DESC
""")

display(df_channel_performance)

# COMMAND ----------

# DBTITLE 1,Write Channel Performance
writeDfToTable(df_channel_performance,"gold","channelperformance")

# COMMAND ----------

# DBTITLE 1,Calculate Product Mix
# ============================================
# Product Mix Distribution
# ============================================

df_product_mix = spark.sql("""
WITH category_totals AS (
    SELECT 
        SUM(PaymentAmount) as GrandTotalRevenue
    FROM insureallBI.silver.factpayments
    WHERE PaymentStatus = 'Success'
)
SELECT 
    PolicyCategory,
    
    COUNT(DISTINCT PolicyID) as UniquePolicies,
    COUNT(DISTINCT TransactionID) as TotalPayments,
    COUNT(DISTINCT CustomerID) as UniqueCustomers,
    SUM(PaymentAmount) as TotalRevenue,
    AVG(PaymentAmount) as AvgPaymentAmount,
    
    ROUND(
        SUM(PaymentAmount) / (SELECT GrandTotalRevenue FROM category_totals) * 100, 2
    ) as RevenueContributionPercent,
    
    CASE 
        WHEN ROUND(SUM(PaymentAmount) / (SELECT GrandTotalRevenue FROM category_totals) * 100, 2) > 30 
            THEN 'Core Product'
        WHEN ROUND(SUM(PaymentAmount) / (SELECT GrandTotalRevenue FROM category_totals) * 100, 2) > 15 
            THEN 'Major Product'
        WHEN ROUND(SUM(PaymentAmount) / (SELECT GrandTotalRevenue FROM category_totals) * 100, 2) > 5 
            THEN 'Supporting Product'
        ELSE 'Niche Product'
    END as ProductClassification,
    
    current_timestamp() as LoadDate
    
FROM insureallBI.silver.factpayments
WHERE PaymentStatus = 'Success'
GROUP BY PolicyCategory
ORDER BY TotalRevenue DESC
""")

display(df_product_mix)

# COMMAND ----------

# DBTITLE 1,Write Product Mix
writeDfToTable(df_product_mix,"gold","productmix")