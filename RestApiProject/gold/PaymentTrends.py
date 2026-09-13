# Databricks notebook source
# MAGIC %run ../misc/Utilities

# COMMAND ----------

# DBTITLE 1,Payment Trends - Overview
# MAGIC %md
# MAGIC # Payment Trends Analysis - Gold Layer
# MAGIC
# MAGIC This notebook analyzes payment patterns over time:
# MAGIC - Monthly, quarterly, and yearly payment aggregations
# MAGIC - Payment volume and revenue trends
# MAGIC - On-time payment rates vs delayed payments
# MAGIC - Payment method preferences
# MAGIC - Payment frequency analysis (monthly, quarterly, yearly)
# MAGIC - MRR (Monthly Recurring Revenue) and ARR (Annual Recurring Revenue)
# MAGIC
# MAGIC **Source Tables:**
# MAGIC - insureallBI.silver.factpayments
# MAGIC
# MAGIC **Target Table:** insureallBI.gold.payment_trends

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG insureallbi

# COMMAND ----------

# DBTITLE 1,Calculate Payment Trends
# ============================================
# Gold Layer - Payment Trends
# ============================================

df_payment_trends = spark.sql("""
SELECT 
    -- Time Dimensions
    PaymentYear,
    PaymentQuarter,
    PaymentMonth,
    PaymentMonthName,
    DATE_TRUNC('month', PaymentDate) as PaymentMonthDate,
    
    -- Payment Context
    PaymentStatus,
    PaymentMode,
    PaymentFrequency,
    
    -- Policy Context
    PolicyCategory,
    
    -- Aggregated Metrics
    COUNT(DISTINCT TransactionID) as TotalPayments,
    COUNT(DISTINCT CustomerID) as UniquePayingCustomers,
    COUNT(DISTINCT PolicyID) as UniquePoliciesPaid,
    
    -- Financial Metrics
    SUM(PaymentAmount) as TotalRevenue,
    round(AVG(PaymentAmount),2) as AvgPaymentAmount,
    MIN(PaymentAmount) as MinPaymentAmount,
    MAX(PaymentAmount) as MaxPaymentAmount,
    
    -- Payment Status Distribution
    COUNT(CASE WHEN PaymentStatus = 'Paid' THEN 1 END) as PaidPayments,
    COUNT(CASE WHEN PaymentStatus = 'Failed' THEN 1 END) as FailedPayments,
    COUNT(CASE WHEN PaymentStatus = 'Pending' THEN 1 END) as PendingPayments,
    COUNT(CASE WHEN PaymentStatus = 'Refunded' THEN 1 END) as RefundedPayments,
    
    ROUND(
        COUNT(CASE WHEN PaymentStatus = 'Paid' THEN 1 END) / 
        CAST(COUNT(*) AS DOUBLE) * 100, 2
    ) as PaymentSuccessRate,
    
       
    -- Recurring Revenue Metrics (for paid payments only)
    SUM(CASE WHEN PaymentStatus = 'Paid' THEN PaymentAmount ELSE 0 END) as MonthlyRecurringRevenue,
    
    SUM(CASE WHEN PaymentStatus = 'Paid' THEN PaymentAmount ELSE 0 END) * 12 as AnnualizedRecurringRevenue,
    
    -- Payment Frequency Analysis
    COUNT(CASE WHEN PaymentFrequency = 'Monthly' THEN 1 END) as MonthlyFrequencyPayments,
    COUNT(CASE WHEN PaymentFrequency = 'Quarterly' THEN 1 END) as QuarterlyFrequencyPayments,
    COUNT(CASE WHEN PaymentFrequency = 'Semi-Annual' THEN 1 END) as SemiAnnualFrequencyPayments,
    COUNT(CASE WHEN PaymentFrequency = 'Annual' THEN 1 END) as AnnualFrequencyPayments,
    
    -- Payment Method Distribution
    COUNT(CASE WHEN PaymentMode = 'Credit Card' THEN 1 END) as CreditCardPayments,
    COUNT(CASE WHEN PaymentMode = 'Debit Card' THEN 1 END) as DebitCardPayments,
    COUNT(CASE WHEN PaymentMode = 'Bank Transfer' THEN 1 END) as BankTransferPayments,
    COUNT(CASE WHEN PaymentMode = 'Cash' THEN 1 END) as CashPayments,
    COUNT(CASE WHEN PaymentMode = 'Check' THEN 1 END) as CheckPayments,
    
    -- Audit
    current_timestamp() as LoadDate
    
FROM silver.factpayments
GROUP BY 
    PaymentYear,
    PaymentQuarter,
    PaymentMonth,
    PaymentMonthName,
    DATE_TRUNC('month', PaymentDate),
    PaymentStatus,
    PaymentMode,
    PaymentFrequency,
    PolicyCategory
ORDER BY PaymentYear DESC, PaymentMonth DESC
""")

display(df_payment_trends)

# COMMAND ----------

# DBTITLE 1,Write to Gold Table
writeDfToTable(df_payment_trends, "gold","payment_trends")