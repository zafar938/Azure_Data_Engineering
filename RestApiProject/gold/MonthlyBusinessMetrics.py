# Databricks notebook source
# MAGIC %run ../misc/Utilities

# COMMAND ----------

# DBTITLE 1,Monthly Business Metrics - Overview
# MAGIC %md
# MAGIC # Monthly Business Metrics - Gold Layer
# MAGIC
# MAGIC This notebook creates a comprehensive monthly KPI dashboard:
# MAGIC - Monthly revenue (premiums collected)
# MAGIC - Monthly claims paid
# MAGIC - New customer acquisitions
# MAGIC - Policy enrollments
# MAGIC - Active policies
# MAGIC - Month-over-month growth rates
# MAGIC - Year-over-year comparisons
# MAGIC - Key profitability indicators
# MAGIC
# MAGIC **Source Tables:**
# MAGIC - insureallBI.silver.factpayments
# MAGIC - insureallBI.silver.factclaims
# MAGIC - insureallBI.bronze.customer_policies
# MAGIC - insureallBI.bronze.insurance_customers
# MAGIC
# MAGIC **Target Table:** insureallBI.gold.monthly_business_metrics

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG insureallbi

# COMMAND ----------

# DBTITLE 1,Calculate Monthly Business Metrics
# ============================================
# Gold Layer - Monthly Business Metrics
# ============================================

df_monthly_business_metrics = spark.sql("""
WITH monthly_revenue AS (
    SELECT 
        YEAR(PaymentDate) as Year,
        MONTH(PaymentDate) as Month,
        DATE_TRUNC('month', PaymentDate) as MonthDate,
        COUNT(DISTINCT TransactionID) as TotalPayments,
        COUNT(DISTINCT CustomerID) as PayingCustomers,
        ROUND(SUM(PaymentAmount),2) as TotalRevenue,
        ROUND(AVG(PaymentAmount),2) as AvgPaymentAmount
    FROM insureallBI.silver.factpayments
    WHERE PaymentStatus = 'Success'
    GROUP BY YEAR(PaymentDate), MONTH(PaymentDate), DATE_TRUNC('month', PaymentDate)
),
monthly_claims AS (
    SELECT 
        YEAR(ClaimDate) as Year,
        MONTH(ClaimDate) as Month,
        DATE_TRUNC('month', ClaimDate) as MonthDate,
        COUNT(DISTINCT ClaimID) as TotalClaims,
        COUNT(DISTINCT CustomerID) as CustomersWithClaims,
        ROUND(SUM(ClaimAmount),2) as TotalClaimAmount,
        ROUND(SUM(SettlementAmount),2) as TotalSettlementAmount,
        SUM(CASE WHEN IsFraudulent = TRUE THEN 1 END) as FraudClaims,
        ROUND(AVG(ProcessingDays),2) as AvgProcessingDays
    FROM insureallBI.silver.factclaims
    GROUP BY YEAR(ClaimDate), MONTH(ClaimDate), DATE_TRUNC('month', ClaimDate)
),
monthly_enrollments AS (
    SELECT 
        YEAR(PolicyEnrollDate) as Year,
        MONTH(PolicyEnrollDate) as Month,
        DATE_TRUNC('month', PolicyEnrollDate) as MonthDate,
        COUNT(*) as NewEnrollments,
        COUNT(DISTINCT CustomerID) as UniqueNewCustomers,
        COUNT(DISTINCT PolicyID) as UniquePoliciesEnrolled
    FROM silver.dimcustomer_policies
    GROUP BY YEAR(PolicyEnrollDate), MONTH(PolicyEnrollDate), DATE_TRUNC('month', PolicyEnrollDate)
),
monthly_metrics AS (
    SELECT 
        COALESCE(mr.Year, mc.Year, me.Year) as Year,
        COALESCE(mr.Month, mc.Month, me.Month) as Month,
        COALESCE(mr.MonthDate, mc.MonthDate, me.MonthDate) as MonthDate,
        DATE_FORMAT(COALESCE(mr.MonthDate, mc.MonthDate, me.MonthDate), 'MMMM') as MonthName,
        
        -- Revenue Metrics
        COALESCE(mr.TotalPayments, 0) as TotalPayments,
        COALESCE(mr.PayingCustomers, 0) as PayingCustomers,
        ROUND(COALESCE(mr.TotalRevenue, 0),2) as TotalRevenue,
        ROUND(COALESCE(mr.AvgPaymentAmount, 0),2) as AvgPaymentAmount,
        
        -- Claims Metrics
        COALESCE(mc.TotalClaims, 0) as TotalClaims,
        COALESCE(mc.CustomersWithClaims, 0) as CustomersWithClaims,
        COALESCE(mc.TotalClaimAmount, 0) as TotalClaimAmount,
        COALESCE(mc.TotalSettlementAmount, 0) as TotalSettlementPaid,
        COALESCE(mc.FraudClaims, 0) as FraudClaims,
        COALESCE(mc.AvgProcessingDays, 0) as AvgClaimProcessingDays,
        
        -- Enrollment Metrics
        COALESCE(me.NewEnrollments, 0) as NewEnrollments,
        COALESCE(me.UniqueNewCustomers, 0) as NewCustomersAcquired,
        COALESCE(me.UniquePoliciesEnrolled, 0) as UniquePoliciesEnrolled,
        
        -- Calculated Profitability Metrics
        Round(COALESCE(mr.TotalRevenue, 0) - COALESCE(mc.TotalSettlementAmount, 0),2) as NetProfit,
        
        CASE 
            WHEN mr.TotalRevenue > 0
            THEN ROUND(mc.TotalSettlementAmount / mr.TotalRevenue * 100, 2)
            ELSE 0
        END as LossRatio,
        
        CASE 
            WHEN mr.PayingCustomers > 0
            THEN ROUND(mr.TotalRevenue / mr.PayingCustomers, 2)
            ELSE 0
        END as RevenuePerCustomer,
        
        CASE 
            WHEN mc.CustomersWithClaims > 0
            THEN ROUND(mc.TotalClaims / CAST(mc.CustomersWithClaims AS DOUBLE), 2)
            ELSE 0
        END as ClaimsPerCustomer
        
    FROM monthly_revenue mr
    FULL OUTER JOIN monthly_claims mc 
        ON mr.Year = mc.Year AND mr.Month = mc.Month
    FULL OUTER JOIN monthly_enrollments me 
        ON COALESCE(mr.Year, mc.Year) = me.Year 
        AND COALESCE(mr.Month, mc.Month) = me.Month
)
SELECT 
    *,
    
    -- Month-over-Month Growth (calculated using LAG window function)
    LAG(TotalRevenue, 1) OVER (ORDER BY Year, Month) as PrevMonthRevenue,
    
    CASE 
        WHEN LAG(TotalRevenue, 1) OVER (ORDER BY Year, Month) > 0
        THEN ROUND(
            (TotalRevenue - LAG(TotalRevenue, 1) OVER (ORDER BY Year, Month)) / 
            LAG(TotalRevenue, 1) OVER (ORDER BY Year, Month) * 100, 2
        )
        ELSE NULL
    END as MoMRevenueGrowthPercent,
    
    -- Year-over-Year Comparison
    LAG(TotalRevenue, 12) OVER (ORDER BY Year, Month) as PrevYearRevenue,
    
    CASE 
        WHEN LAG(TotalRevenue, 12) OVER (ORDER BY Year, Month) > 0
        THEN ROUND(
            (TotalRevenue - LAG(TotalRevenue, 12) OVER (ORDER BY Year, Month)) / 
            LAG(TotalRevenue, 12) OVER (ORDER BY Year, Month) * 100, 2
        )
        ELSE NULL
    END as YoYRevenueGrowthPercent,
    
    -- Audit
    current_timestamp() as LoadDate
    
FROM monthly_metrics
ORDER BY Year DESC, Month DESC
""")

display(df_monthly_business_metrics)

# COMMAND ----------

# DBTITLE 1,Write to Gold Table
writeDfToTable(df_monthly_business_metrics, "gold", "businessmetrics")