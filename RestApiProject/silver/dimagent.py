# Databricks notebook source
# DBTITLE 1,Run Utilities
# MAGIC %run ../misc/Utilities

# COMMAND ----------

# DBTITLE 1,Set catalog
# MAGIC %sql
# MAGIC USE CATALOG insureallBI

# COMMAND ----------

# DBTITLE 1,Set Configs
# ============================================
# Configuration - Update these values
# ============================================
SOURCE_TABLE = "insureallBI.bronze.insurance_agents"  
TARGET_TABLE = "insureallBI.silver.dimagent"

# COMMAND ----------

# DBTITLE 1,Read Bronze Table


# ============================================
# Read Bronze Data
# ============================================
df_agent = spark.table(SOURCE_TABLE)
display(df_agent)

# COMMAND ----------

# DBTITLE 1,Silver Transformation Logic with SQL
# ============================================
# Silver Transformation Logic for Agent Dimension
# ============================================

df_silver_agent = spark.sql("""
SELECT 
    -- Hash Key (Surrogate Key)
    MD5(CONCAT(CAST(agent_id AS STRING), '|', CAST(date_of_joining AS STRING))) as AgentHashKey,
    
    -- Agent Basic Information
    agent_id as AgentID,
    agent_name as AgentName,
    agent_email as AgentEmail,
    agent_phone as AgentPhone,
    agent_gender as Gender,
    agent_status as Status,
    agent_type as AgentType,
    
    -- License Information
    license_number as LicenseNumber,
    TO_DATE(license_expiry_date, 'dd-MM-yyyy') as LicenseExpiryDate,
    CASE 
        WHEN TO_DATE(license_expiry_date, 'dd-MM-yyyy') < current_date() THEN TRUE 
        ELSE FALSE 
    END as IsLicenseExpired,
    kyc_verified as KYCVerified,
    background_check_status as BackgroundCheckStatus,
    
    -- Address fields
    agent_address as Address,
    city as City,
    state as State,
    country as Country,
    zone as Zone,
    region_id as RegionID,
    
    -- Organizational Structure
    branch_id as BranchID,
    branch_name as BranchName,
    manager_agent_id as ManagerAgentID,
    sales_team as SalesTeam,
    
    -- Date fields with proper conversion
    TO_DATE(date_of_birth, 'dd-MM-yyyy') as DateOfBirth,
    date_of_joining as DateOfJoining,
    
    -- Age calculation
    CAST(DATEDIFF(current_date(), TO_DATE(date_of_birth, 'dd-MM-yyyy')) / 365.25 AS INT) as Age,
    
    -- Age group categorization
    CASE 
        WHEN CAST(DATEDIFF(current_date(), TO_DATE(date_of_birth, 'dd-MM-yyyy')) / 365.25 AS INT) < 25 THEN 'Young (<25)'
        WHEN CAST(DATEDIFF(current_date(), TO_DATE(date_of_birth, 'dd-MM-yyyy')) / 365.25 AS INT) BETWEEN 25 AND 35 THEN 'Early Career (25-35)'
        WHEN CAST(DATEDIFF(current_date(), TO_DATE(date_of_birth, 'dd-MM-yyyy')) / 365.25 AS INT) BETWEEN 36 AND 45 THEN 'Mid Career (36-45)'
        WHEN CAST(DATEDIFF(current_date(), TO_DATE(date_of_birth, 'dd-MM-yyyy')) / 365.25 AS INT) BETWEEN 46 AND 55 THEN 'Senior (46-55)'
        ELSE 'Veteran (55+)'
    END as AgeGroup,
    
    -- Experience and tenure
    experience_years as ExperienceYears,
    DATEDIFF(current_date(), date_of_joining) as TenureDays,
    CAST(DATEDIFF(current_date(), date_of_joining) / 365.25 AS INT) as TenureYears,
    
    -- Experience category
    CASE 
        WHEN experience_years < 2 THEN 'Novice (< 2 years)'
        WHEN experience_years BETWEEN 2 AND 5 THEN 'Intermediate (2-5 years)'
        WHEN experience_years BETWEEN 6 AND 10 THEN 'Experienced (6-10 years)'
        WHEN experience_years BETWEEN 11 AND 15 THEN 'Senior (11-15 years)'
        ELSE 'Expert (15+ years)'
    END as ExperienceCategory,
    
    -- Performance Metrics
    total_policies_sold as TotalPoliciesSold,
    policies_sold_current_year as PoliciesSoldCurrentYear,
    policies_sold_last_year as PoliciesSoldLastYear,
    avg_policy_value as AvgPolicyValue,
    
    -- Growth calculation
    CASE 
        WHEN policies_sold_last_year > 0 THEN 
            ROUND(((policies_sold_current_year - policies_sold_last_year) / policies_sold_last_year) * 100, 2)
        ELSE NULL
    END as YearOverYearGrowthPct,
    
    -- Performance category
    CASE 
        WHEN total_policies_sold < 100 THEN 'Low Performer'
        WHEN total_policies_sold BETWEEN 100 AND 500 THEN 'Average Performer'
        WHEN total_policies_sold BETWEEN 501 AND 1000 THEN 'High Performer'
        ELSE 'Top Performer'
    END as PerformanceCategory,
    
    -- Commission Information
    commission_rate as CommissionRate,
    total_commission_earned as TotalCommissionEarned,
    commission_paid_ytd as CommissionPaidYTD,
    commission_pending as CommissionPending,
    
    -- Quality Metrics
    conversion_rate as ConversionRate,
    customer_retention_rate as CustomerRetentionRate,
    customer_satisfaction_score as CustomerSatisfactionScore,
    compliance_score as ComplianceScore,
    rating as Rating,
    
    -- Service Metrics
    avg_response_time_minutes as AvgResponseTimeMinutes,
    complaints_handled as ComplaintsHandled,
    escalations_count as EscalationsCount,
    
    -- Activity Metrics
    login_count_30_days as LoginCount30Days,
    last_login_channel as LastLoginChannel,
    device_type as DeviceType,
    TO_TIMESTAMP(REPLACE(last_activity_timestamp, 'T', ' '), 'dd-MM-yyyy HH:mm:ss') as LastActivityTimestamp,
    
    -- Activity status
    CASE 
        WHEN DATEDIFF(current_date(), TO_DATE(REPLACE(last_activity_timestamp, 'T', ' '), 'dd-MM-yyyy HH:mm:ss')) <= 7 THEN 'Active'
        WHEN DATEDIFF(current_date(), TO_DATE(REPLACE(last_activity_timestamp, 'T', ' '), 'dd-MM-yyyy HH:mm:ss')) <= 30 THEN 'Moderate'
        ELSE 'Inactive'
    END as ActivityStatus,
    
    -- Audit Information
    TO_DATE(last_audit_date, 'dd-MM-yyyy') as LastAuditDate,
    TO_DATE(last_commission_date, 'dd-MM-yyyy') as LastCommissionDate,
    TO_TIMESTAMP(REPLACE(record_created_timestamp, 'T', ' '), 'dd-MM-yyyy HH:mm:ss') as RecordCreatedTimestamp,
    TO_TIMESTAMP(REPLACE(record_updated_timestamp, 'T', ' '), 'dd-MM-yyyy HH:mm:ss') as RecordUpdatedTimestamp,
    
    -- Data quality flags
    CASE 
        WHEN agent_email IS NULL OR agent_email = '' THEN FALSE
        WHEN agent_email LIKE '%@%' THEN TRUE
        ELSE FALSE
    END as HasValidEmail,
    
    CASE 
        WHEN agent_phone IS NULL OR agent_phone = '' THEN FALSE
        WHEN LENGTH(agent_phone) >= 10 THEN TRUE
        ELSE FALSE
    END as HasValidPhone,
    
    -- Joining time analysis
    YEAR(date_of_joining) as JoiningYear,
    QUARTER(date_of_joining) as JoiningQuarter,
    MONTH(date_of_joining) as JoiningMonth,
    DATE_FORMAT(date_of_joining, 'MMMM') as JoiningMonthName,
    
    -- Geographic region mapping (for Indian states)
    CASE 
        WHEN state IN ('Maharashtra', 'Gujarat', 'Goa') THEN 'West'
        WHEN state IN ('Tamil Nadu', 'Karnataka', 'Kerala', 'Andhra Pradesh', 'Telangana') THEN 'South'
        WHEN state IN ('Uttar Pradesh', 'Delhi', 'Haryana', 'Punjab', 'Rajasthan', 'Uttarakhand') THEN 'North'
        WHEN state IN ('West Bengal', 'Bihar', 'Jharkhand', 'Odisha', 'Sikkim', 'Assam') THEN 'East'
        WHEN state IN ('Madhya Pradesh', 'Chhattisgarh') THEN 'Central'
        ELSE 'Other'
    END as GeographicRegion,
    
    -- Status flags
    CASE WHEN UPPER(agent_status) = 'ACTIVE' THEN TRUE ELSE FALSE END as IsActive,
    CASE WHEN UPPER(agent_status) = 'SUSPENDED' THEN TRUE ELSE FALSE END as IsSuspended,
    CASE WHEN UPPER(is_deleted) = '1' OR UPPER(is_deleted) = 'TRUE' THEN TRUE ELSE FALSE END as IsDeleted,
    
    -- Load timestamp
    current_timestamp() as LoadTimestamp
    
FROM bronze.insurance_agents
WHERE UPPER(is_deleted) != '1' OR is_deleted IS NULL  -- Exclude deleted records
""")

display(df_silver_agent)

# COMMAND ----------

# DBTITLE 1,Load Agent Data to Silver
loadIncrementalData(df_silver_agent, "silver", "dimagent", "AgentHashKey")

# COMMAND ----------

# DBTITLE 1,Verify Agent Dimension
# MAGIC %sql
# MAGIC SELECT * FROM insureallbi.silver.dimagent LIMIT 100