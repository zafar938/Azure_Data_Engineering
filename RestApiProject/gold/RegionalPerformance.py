# Databricks notebook source
# MAGIC %run ../misc/Utilities

# COMMAND ----------

# DBTITLE 1,Regional Performance - Overview
# MAGIC %md
# MAGIC # Regional Performance Analysis - Gold Layer
# MAGIC
# MAGIC This notebook analyzes business performance by geography:
# MAGIC - Premium collection by country/region
# MAGIC - Claim frequency and amounts by geography
# MAGIC - Customer density and market penetration
# MAGIC - Regional profitability analysis
# MAGIC - Growth trends by region
# MAGIC - Risk profiles by geography
# MAGIC
# MAGIC **Source Tables:**
# MAGIC - insureallBI.silver.factpayments
# MAGIC - insureallBI.silver.factclaims
# MAGIC - insureallBI.bronze.insurance_countries
# MAGIC - insureallBI.bronze.insurance_customers
# MAGIC
# MAGIC **Target Table:** insureallBI.gold.regional_performance

# COMMAND ----------

# DBTITLE 1,Set catalog
# MAGIC %sql
# MAGIC USE CATALOG insureallbi

# COMMAND ----------

# MAGIC %sql
# MAGIC  SELECT 
# MAGIC         co.country_id as CountryID,
# MAGIC         co.country_name as CountryName,
# MAGIC         COUNT(DISTINCT p.TransactionID) as TotalPayments,
# MAGIC         COUNT(DISTINCT c.CustomerID) as TotalCustomers,
# MAGIC         SUM(p.PaymentAmount) as TotalRevenue,
# MAGIC         ROUND(AVG(p.PaymentAmount),2) as AvgPaymentAmount,
# MAGIC         AVG(c.age) as AvgCustomerAge,
# MAGIC         COUNT(CASE WHEN c.gender = 'Male' THEN 1 END) as MaleCustomers,
# MAGIC         COUNT(CASE WHEN c.gender = 'Female' THEN 1 END) as FemaleCustomers
# MAGIC     FROM silver.dimcustomer c
# MAGIC     LEFT JOIN silver.dimcountries co ON c.country = co.country_name
# MAGIC     LEFT JOIN silver.factpayments p  ON p.CustomerID = c.CustomerID
# MAGIC     GROUP BY co.country_id, co.country_name

# COMMAND ----------

# DBTITLE 1,Calculate Regional Performance
# ============================================
# Gold Layer - Regional Performance
# ============================================

df_regional_performance = spark.sql("""
WITH country_customers AS (
    SELECT 
        co.country_id as CountryID,
        co.country_name as CountryName,
        COUNT(DISTINCT p.TransactionID) as TotalPayments,
        COUNT(DISTINCT c.CustomerID) as TotalCustomers,
        SUM(p.PaymentAmount) as TotalRevenue,
        ROUND(AVG(p.PaymentAmount),2) as AvgPaymentAmount,
        AVG(c.age) as AvgCustomerAge,
        COUNT(CASE WHEN c.gender = 'Male' THEN 1 END) as MaleCustomers,
        COUNT(CASE WHEN c.gender = 'Female' THEN 1 END) as FemaleCustomers
    FROM silver.dimcustomer c
    INNER JOIN silver.dimcountries co ON c.country = co.country_name
    INNER JOIN silver.factpayments p  ON p.CustomerID = c.CustomerID
    GROUP BY co.country_id, co.country_name
),
country_claims AS (
    SELECT 
        c.CountryID,
        COUNT(DISTINCT c.ClaimID) as TotalClaims,
        COUNT(DISTINCT c.CustomerID) as CustomersWithClaims,
        SUM(c.ClaimAmount) as TotalClaimAmount,
        SUM(c.SettlementAmount) as TotalSettlementAmount,
        AVG(c.ProcessingDays) as AvgProcessingDays,
        SUM(CASE WHEN c.IsFraudulent = TRUE THEN 1 END) as FraudClaimCount
    FROM insureallBI.silver.factclaims c
    GROUP BY c.CountryID
)
SELECT 
    -- Geographic Information
    cc.CountryID,
    cc.CountryName,
    
    -- Customer Metrics
    cc.TotalCustomers,
    ROUND(cc.AvgCustomerAge, 1) as AvgCustomerAge,
    cc.MaleCustomers,
    cc.FemaleCustomers,
    ROUND(cc.MaleCustomers / CAST(cc.TotalCustomers AS DOUBLE) * 100, 2) as MaleCustomerPercent,
    
    -- Payment/Revenue Metrics
    COALESCE(cc.TotalPayments, 0) as TotalPayments,
    COALESCE(cc.TotalRevenue, 0) as TotalRevenue,
    COALESCE(cc.AvgPaymentAmount, 0) as AvgPaymentAmount,
      -- Claims Metrics
    COALESCE(ccl.TotalClaims, 0) as TotalClaims,
    COALESCE(ccl.CustomersWithClaims, 0) as CustomersWithClaims,
    COALESCE(ccl.TotalClaimAmount, 0) as TotalClaimAmount,
    COALESCE(ccl.TotalSettlementAmount, 0) as TotalSettlementAmount,
    COALESCE(ccl.AvgProcessingDays, 0) as AvgClaimProcessingDays,
    COALESCE(ccl.FraudClaimCount, 0) as FraudClaimCount,
    
    -- Calculated Metrics
    COALESCE(cc.TotalRevenue, 0) - COALESCE(ccl.TotalSettlementAmount, 0) as NetProfitability,
    
    CASE 
        WHEN cc.TotalRevenue > 0
        THEN ROUND(ccl.TotalSettlementAmount / cc.TotalRevenue * 100, 2)
        ELSE 0
    END as LossRatio,
    
    CASE 
        WHEN cc.TotalCustomers > 0
        THEN ROUND(cc.TotalCustomers / CAST(cc.TotalCustomers AS DOUBLE) * 100, 2)
        ELSE 0
    END as CustomerActivationRate,
    
    CASE 
        WHEN cc.TotalCustomers > 0
        THEN ROUND(ccl.CustomersWithClaims / CAST(cc.TotalCustomers AS DOUBLE) * 100, 2)
        ELSE 0
    END as ClaimFrequencyRate,
    
    CASE 
        WHEN cc.TotalCustomers > 0
        THEN ROUND(cc.TotalRevenue / cc.TotalCustomers, 2)
        ELSE 0
    END as RevenuePerCustomer,
    
    CASE 
        WHEN ccl.CustomersWithClaims > 0
        THEN ROUND(ccl.TotalSettlementAmount / ccl.CustomersWithClaims, 2)
        ELSE 0
    END as AvgSettlementPerCustomer,
    
    -- Risk & Performance Categorization
    CASE 
        WHEN COALESCE(ccl.TotalSettlementAmount, 0) / NULLIF(cc.TotalRevenue, 0) > 0.8 THEN 'High Risk Region'
        WHEN COALESCE(ccl.TotalSettlementAmount, 0) / NULLIF(cc.TotalRevenue, 0) > 0.6 THEN 'Medium Risk Region'
        ELSE 'Low Risk Region'
    END as RegionRiskProfile,
    
    CASE 
        WHEN COALESCE(cc.TotalRevenue, 0) - COALESCE(ccl.TotalSettlementAmount, 0) > 1000000 THEN 'Top Performing'
        WHEN COALESCE(cc.TotalRevenue, 0) - COALESCE(ccl.TotalSettlementAmount, 0) > 500000 THEN 'High Performing'
        WHEN COALESCE(cc.TotalRevenue, 0) - COALESCE(ccl.TotalSettlementAmount, 0) > 100000 THEN 'Average Performing'
        WHEN COALESCE(cc.TotalRevenue, 0) - COALESCE(ccl.TotalSettlementAmount, 0) > 0 THEN 'Low Performing'
        ELSE 'Loss Making'
    END as PerformanceCategory,
    
    -- Market Penetration Score (Revenue per customer as proxy)
    CASE 
        WHEN ROUND(cc.TotalRevenue / NULLIF(cc.TotalCustomers, 0), 2) > 5000 THEN 'High Penetration'
        WHEN ROUND(cc.TotalRevenue / NULLIF(cc.TotalCustomers, 0), 2) > 2000 THEN 'Medium Penetration'
        ELSE 'Low Penetration'
    END as MarketPenetrationLevel,
    
    -- Audit
    current_timestamp() as LoadDate
    
FROM country_customers cc
LEFT JOIN country_claims ccl ON cc.CountryID = ccl.CountryID
""")

display(df_regional_performance)

# COMMAND ----------

# DBTITLE 1,Write to Gold Table
writeDfToTable(df_regional_performance,"gold","regional_performance")