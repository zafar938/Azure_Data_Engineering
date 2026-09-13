# Databricks notebook source
# DBTITLE 1,Insurance Analytics Platform - REST API Data Pipeline
# MAGIC %md
# MAGIC # Insurance Analytics Platform - REST API Data Pipeline
# MAGIC
# MAGIC ## Project Overview
# MAGIC Implemented a complete **Medallion Architecture (Bronze-Silver-Gold)** data pipeline for an insurance analytics platform on Databricks, ingesting data from REST APIs and transforming it into actionable business intelligence.
# MAGIC
# MAGIC ## Technical Implementation
# MAGIC
# MAGIC **Bronze Layer - Data Ingestion:**
# MAGIC * Developed REST API integration with pagination handling to fetch insurance datasets (customers, policies, claims, payments, agents, countries)
# MAGIC * Stored raw data as Delta tables in `insureallBI.bronze` catalog
# MAGIC * Implemented error handling and logging for pipeline monitoring
# MAGIC
# MAGIC **Silver Layer - Data Transformation:**
# MAGIC * Built dimensional data model with fact and dimension tables (dimcustomer, dimpolicies, factpayments, factclaims)
# MAGIC * Applied data quality rules: created hash keys for surrogate keys, standardized formats, derived metrics (age groups, tenure categories, occupation classification)
# MAGIC * Implemented incremental MERGE operations for efficient data loading
# MAGIC * Added data validation checks (email/phone validation, regional classification)
# MAGIC
# MAGIC **Gold Layer - Business Analytics:**
# MAGIC * Created aggregated business metrics: monthly revenue, claims analysis, customer lifetime value, policy profitability, regional performance
# MAGIC * Calculated KPIs: loss ratios, MoM/YoY growth rates, payment trends, fraud detection metrics
# MAGIC * Enabled executive dashboards with ready-to-query analytical tables
# MAGIC
# MAGIC **Tech Stack:** PySpark, Databricks SQL, Delta Lake, REST API integration, medallion architecture
# MAGIC
# MAGIC **Business Impact:** Enabled data-driven decision-making with 360° customer view, real-time profitability tracking, and fraud detection capabilities.

# COMMAND ----------

# DBTITLE 1,Utilities Module Overview
# MAGIC %md
# MAGIC ## Utilities Module - Reusable Pipeline Functions
# MAGIC
# MAGIC **Location:** `/RestApi/RestApiProject/misc/Utilities`
# MAGIC
# MAGIC ### Core Functions:
# MAGIC
# MAGIC 1. **`fetch_rest_api_dataset()`** - REST API integration with pagination, authentication, and retry logic
# MAGIC 2. **`clean_dataset()`** - Data quality: trim whitespace, standardize nulls, remove duplicates
# MAGIC 3. **`apply_schema()`** - Type enforcement using schema_registry
# MAGIC 4. **`loadIncrementalData()`** - Smart MERGE operations for incremental loads
# MAGIC 5. **`log_pipeline_status()`** - Centralized error logging
# MAGIC 6. **`writeDfToTable()`** - Delta table writes with schema evolution
# MAGIC
# MAGIC **Benefits:** Reusability, maintainability, consistency across bronze/silver/gold layers

# COMMAND ----------

# DBTITLE 1,Technical Deep Dive - MERGE, Retry, and Schema Registry
# MAGIC %md
# MAGIC ## Technical Deep Dive - Key Utilities Components
# MAGIC
# MAGIC ### 1. MERGE Operation Logic (`loadIncrementalData`)
# MAGIC
# MAGIC **Purpose:** Efficiently handle incremental data loads without full table rewrites
# MAGIC
# MAGIC **Logic Flow:**
# MAGIC ```
# MAGIC IF table does NOT exist:
# MAGIC     ➜ Full initial load (overwrite mode)
# MAGIC ELSE:
# MAGIC     ➜ MERGE operation (UPDATE existing + INSERT new)
# MAGIC ```
# MAGIC
# MAGIC **MERGE SQL Structure:**
# MAGIC ```sql
# MAGIC MERGE INTO target_table AS target
# MAGIC USING source_view AS source
# MAGIC ON target.mergeKey = source.mergeKey
# MAGIC WHEN MATCHED THEN
# MAGIC     UPDATE SET target.col1 = source.col1, ...
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (col1, col2, ...) VALUES (source.col1, source.col2, ...)
# MAGIC ```
# MAGIC
# MAGIC **Dynamic SQL Generation:**
# MAGIC * Automatically extracts all DataFrame columns
# MAGIC * Builds UPDATE SET clause dynamically (excludes merge key from updates)
# MAGIC * Constructs INSERT columns and VALUES lists
# MAGIC * Creates temporary view for source data
# MAGIC
# MAGIC **Benefits:**
# MAGIC * **Idempotent** - Safe to re-run without duplicates
# MAGIC * **Efficient** - Only updates changed records
# MAGIC * **Flexible** - Works with any DataFrame schema via dynamic SQL
# MAGIC
# MAGIC **Example Usage:** `loadIncrementalData(df, 'silver', 'dimcustomer', 'CustomerHashKey')`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 2. Retry Mechanism (`fetch_rest_api_dataset`)
# MAGIC
# MAGIC **Problem:** REST APIs have rate limits (429 status code) causing failures
# MAGIC
# MAGIC **Solution:** Exponential backoff retry strategy
# MAGIC
# MAGIC **Retry Logic:**
# MAGIC ```
# MAGIC Attempt 1: Make request
# MAGIC     ➜ Success (200) → Return data
# MAGIC     ➜ Rate limit (429) → Wait 60 seconds, retry
# MAGIC
# MAGIC Attempt 2: Make request again
# MAGIC     ➜ Success (200) → Return data
# MAGIC     ➜ Rate limit (429) → Wait 300 seconds (5 min), retry
# MAGIC
# MAGIC Attempt 3: Final attempt
# MAGIC     ➜ Success (200) → Return data
# MAGIC     ➜ Rate limit (429) → FAIL pipeline with error
# MAGIC ```
# MAGIC
# MAGIC **Implementation Details:**
# MAGIC * **Max attempts:** 3
# MAGIC * **Backoff schedule:** 1 min → 5 min → fail
# MAGIC * **Error handling:** Catches 429 status codes and "rate_limit_exceeded" messages
# MAGIC * **User feedback:** Prints warnings at each retry stage
# MAGIC
# MAGIC **Why Exponential Backoff:**
# MAGIC * Gives API time to reset rate limits
# MAGIC * Prevents aggressive retries that worsen the problem
# MAGIC * Industry best practice for API resilience
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 3. Schema Registry Structure
# MAGIC
# MAGIC **Purpose:** Centralized type definitions for all datasets to ensure data quality
# MAGIC
# MAGIC **Structure:** Python dictionary mapping dataset names to column type schemas
# MAGIC
# MAGIC **Example Schema Definition:**
# MAGIC ```python
# MAGIC schema_registry = {
# MAGIC     "insurance_customers": {
# MAGIC         "customer_id": IntegerType(),
# MAGIC         "name": StringType(),
# MAGIC         "email": StringType(),
# MAGIC         "datesignedup": TimestampType(),
# MAGIC         "is_active": BooleanType(),
# MAGIC         "pincode": IntegerType(),
# MAGIC         ...
# MAGIC     },
# MAGIC     "insurance_policies": {
# MAGIC         "policy_code": StringType(),
# MAGIC         "base_premium_usd": DoubleType(),
# MAGIC         "is_active": BooleanType(),
# MAGIC         "start_date": TimestampType(),
# MAGIC         ...
# MAGIC     },
# MAGIC     "insurance_claims": {...},
# MAGIC     "insurance_payments": {...}
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC **Supported Types:**
# MAGIC * **StringType()** - Text data
# MAGIC * **IntegerType()** - Whole numbers
# MAGIC * **DoubleType()** - Decimals
# MAGIC * **DecimalType()** - Precise financial values
# MAGIC * **BooleanType()** - True/False flags
# MAGIC * **TimestampType()** - Date/time values
# MAGIC * **LongType()** - Large integers
# MAGIC
# MAGIC **Usage Flow:**
# MAGIC 1. `fetch_rest_api_dataset()` retrieves raw JSON data
# MAGIC 2. Creates initial Spark DataFrame (all strings)
# MAGIC 3. Calls `clean_dataset()` for basic cleaning
# MAGIC 4. Calls `apply_schema()` to cast columns based on registry
# MAGIC 5. Returns strongly-typed DataFrame
# MAGIC
# MAGIC **Benefits:**
# MAGIC * **Type safety** - Prevents downstream errors from wrong types
# MAGIC * **Documentation** - Schema serves as data contract
# MAGIC * **Consistency** - Same types across all pipeline stages
# MAGIC * **Validation** - Catches data quality issues early (failed casts → nulls)
# MAGIC * **Scalability** - Add new datasets by extending registry
# MAGIC
# MAGIC **Interview Talking Points:**
# MAGIC * Demonstrates understanding of data governance
# MAGIC * Shows proactive approach to data quality
# MAGIC * Highlights schema evolution considerations
# MAGIC * Emphasizes importance of early type enforcement in ETL

# COMMAND ----------

# DBTITLE 1,50 Interview Questions - Project & Optimizations
# MAGIC %md
# MAGIC ## 50 Interview Questions - Insurance Analytics Platform
# MAGIC
# MAGIC ### Architecture & Design (1-10)
# MAGIC
# MAGIC 1. **What is the Medallion Architecture and why did you choose it for this project?**
# MAGIC    - Bronze: Raw data ingestion from REST APIs
# MAGIC    - Silver: Cleaned, transformed dimensional model
# MAGIC    - Gold: Aggregated business metrics
# MAGIC    - Benefits: Clear data lineage, separation of concerns, incremental complexity
# MAGIC
# MAGIC 2. **How does your pipeline implement the separation of concerns principle?**
# MAGIC    - Bronze: Ingestion only, no transformations
# MAGIC    - Silver: Business logic and data quality
# MAGIC    - Gold: Analytics and aggregations
# MAGIC    - Utilities: Reusable functions
# MAGIC
# MAGIC 3. **Why did you use Delta Lake instead of Parquet?**
# MAGIC    - ACID transactions for data consistency
# MAGIC    - Time travel for auditing
# MAGIC    - Schema evolution support
# MAGIC    - MERGE operations for incremental loads
# MAGIC    - Better performance with Z-ordering and data skipping
# MAGIC
# MAGIC 4. **Explain your dimensional modeling approach in the Silver layer.**
# MAGIC    - Fact tables: factpayments, factclaims (transactional data)
# MAGIC    - Dimension tables: dimcustomer, dimpolicies, dimagent (descriptive attributes)
# MAGIC    - Star schema for efficient querying
# MAGIC    - Hash keys as surrogate keys for tracking changes
# MAGIC
# MAGIC 5. **How would you handle slowly changing dimensions (SCD) in this pipeline?**
# MAGIC    - SCD Type 1: Overwrite (current implementation)
# MAGIC    - SCD Type 2: Add start_date, end_date, is_current flag
# MAGIC    - Use hash keys to detect changes
# MAGIC    - MERGE logic can be extended to handle historical tracking
# MAGIC
# MAGIC 6. **What data governance practices did you implement?**
# MAGIC    - Centralized schema registry for type enforcement
# MAGIC    - Unity Catalog for data organization (catalog.schema.table)
# MAGIC    - Error logging table for monitoring
# MAGIC    - Data quality checks (email/phone validation)
# MAGIC    - Standardized naming conventions
# MAGIC
# MAGIC 7. **How does your pipeline ensure idempotency?**
# MAGIC    - MERGE operations use hash keys to prevent duplicates
# MAGIC    - Safe to re-run without creating duplicate records
# MAGIC    - First load: overwrite, subsequent loads: upsert
# MAGIC    - Error logging without breaking pipeline flow
# MAGIC
# MAGIC 8. **Describe the data lineage in your pipeline.**
# MAGIC    - REST API → Bronze (raw) → Silver (dimensional) → Gold (analytical)
# MAGIC    - Each layer references previous layer tables
# MAGIC    - Clear transformation logic documented in notebooks
# MAGIC    - LoadTimestamp columns for tracking data freshness
# MAGIC
# MAGIC 9. **Why did you separate Utilities into a separate notebook?**
# MAGIC    - DRY principle: Don't Repeat Yourself
# MAGIC    - Single source of truth for common functions
# MAGIC    - Easier maintenance and testing
# MAGIC    - Imported via `%run ../misc/Utilities` in all notebooks
# MAGIC
# MAGIC 10. **How would you implement data quality checks at scale?**
# MAGIC     - Great Expectations library for validation rules
# MAGIC     - Quarantine tables for failed records
# MAGIC     - Data quality metrics in Gold layer
# MAGIC     - Automated alerts on threshold breaches
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Bronze Layer - Data Ingestion (11-20)
# MAGIC
# MAGIC 11. **Walk me through your REST API integration implementation.**
# MAGIC     - Pagination handling to fetch all pages
# MAGIC     - Basic Authentication with Base64 encoding
# MAGIC     - Error handling for API failures
# MAGIC     - Rate limiting with exponential backoff
# MAGIC     - Converts JSON to Spark DataFrame
# MAGIC
# MAGIC 12. **How do you handle API pagination efficiently?**
# MAGIC     - First request gets total_pages metadata
# MAGIC     - Loop through all pages sequentially
# MAGIC     - Accumulate rows in list, convert once to DataFrame
# MAGIC     - Avoids multiple small DataFrame unions (expensive)
# MAGIC
# MAGIC 13. **Explain your retry mechanism for API rate limiting.**
# MAGIC     - 3 attempts with exponential backoff (1 min → 5 min)
# MAGIC     - Catches 429 status code
# MAGIC     - Prints user-friendly warnings
# MAGIC     - Fails pipeline after 3 attempts to prevent infinite loops
# MAGIC
# MAGIC 14. **What optimizations would you add to the REST API fetching?**
# MAGIC     - **Parallel page fetching** using ThreadPoolExecutor
# MAGIC     - **Caching** responses to avoid redundant API calls
# MAGIC     - **Checkpointing** to resume from last successful page
# MAGIC     - **Incremental ingestion** with last_modified timestamps
# MAGIC     - **Async requests** using aiohttp for better throughput
# MAGIC
# MAGIC 15. **How do you handle schema changes from the API source?**
# MAGIC     - Schema evolution enabled (`overwriteSchema=true`)
# MAGIC     - Schema registry updated for new fields
# MAGIC     - Bronze layer accepts all fields (schema-on-read)
# MAGIC     - Silver layer validates required fields
# MAGIC
# MAGIC 16. **What data quality issues did you address in Bronze?**
# MAGIC     - Whitespace trimming from strings
# MAGIC     - Null standardization ("", "N/A" → NULL)
# MAGIC     - Duplicate record removal
# MAGIC     - Date format standardization
# MAGIC     - Type casting via schema registry
# MAGIC
# MAGIC 17. **How would you implement incremental ingestion in Bronze?**
# MAGIC     - Add watermark column (last_updated timestamp)
# MAGIC     - API filter: `?modified_after=last_watermark`
# MAGIC     - Store watermark in control table
# MAGIC     - Merge new data instead of full reload
# MAGIC     - Reduces API load and processing time
# MAGIC
# MAGIC 18. **Explain the error logging mechanism.**
# MAGIC     - `log_pipeline_status()` writes to logs.pipelineruns table
# MAGIC     - Captures: schema, table, error message, timestamp
# MAGIC     - Allows pipeline to continue after logging
# MAGIC     - Enables monitoring dashboard on failed pipelines
# MAGIC
# MAGIC 19. **How do you ensure data freshness in Bronze tables?**
# MAGIC     - Schedule notebooks to run at regular intervals
# MAGIC     - Add LoadTimestamp column to track ingestion time
# MAGIC     - Monitor lag between API updates and table updates
# MAGIC     - Alerts on stale data thresholds
# MAGIC
# MAGIC 20. **What monitoring would you implement for API ingestion?**
# MAGIC     - API response time metrics
# MAGIC     - Success/failure rate per dataset
# MAGIC     - Row counts and data volume trends
# MAGIC     - Rate limit hit frequency
# MAGIC     - Cost per API call tracking
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Silver Layer - Transformations (21-30)
# MAGIC
# MAGIC 21. **How did you design your dimensional model?**
# MAGIC     - Identified business processes (payments, claims)
# MAGIC     - Created fact tables for transactions
# MAGIC     - Built dimension tables for entities (customers, policies)
# MAGIC     - Used MD5 hash keys for surrogate keys
# MAGIC     - Normalized to avoid data redundancy
# MAGIC
# MAGIC 22. **Explain the hash key generation strategy.**
# MAGIC     - `MD5(CONCAT(customer_id, '|', start_date))` → CustomerHashKey
# MAGIC     - Unique identifier across all sources
# MAGIC     - Enables change detection for SCD Type 2
# MAGIC     - Deterministic: same input = same hash
# MAGIC
# MAGIC 23. **What business logic did you implement in Silver transformations?**
# MAGIC     - Age calculation from date of birth
# MAGIC     - Age group categorization (18-25, 26-35, etc.)
# MAGIC     - Customer tenure in days
# MAGIC     - Tenure category (New, Regular, Long-term, Loyal)
# MAGIC     - Occupation grouping into categories
# MAGIC     - Email domain extraction
# MAGIC     - Data quality flags (HasValidEmail, HasValidPhone)
# MAGIC
# MAGIC 24. **How does your MERGE operation handle updates vs inserts?**
# MAGIC     - MATCHED: Updates all columns except merge key
# MAGIC     - NOT MATCHED: Inserts entire new record
# MAGIC     - Based on hash key comparison
# MAGIC     - Prevents duplicates while capturing changes
# MAGIC
# MAGIC 25. **What optimizations did you apply to Silver transformations?**
# MAGIC     - **Broadcast joins** for small dimension tables
# MAGIC     - **Partitioning** by date columns for query performance
# MAGIC     - **Z-ordering** on frequently filtered columns
# MAGIC     - **Column pruning** to select only needed columns
# MAGIC     - **Predicate pushdown** in WHERE clauses
# MAGIC
# MAGIC 26. **How would you optimize the MERGE operation?**
# MAGIC     - **Partition pruning**: Only MERGE relevant partitions
# MAGIC     - **Compact files**: OPTIMIZE table after MERGE
# MAGIC     - **Statistics**: Run ANALYZE TABLE for query planning
# MAGIC     - **Liquid clustering**: Auto-optimize layout
# MAGIC     - **Increase shuffle partitions** for large datasets
# MAGIC
# MAGIC 27. **Explain your approach to derived metrics.**
# MAGIC     - Calculated at Silver layer for reusability
# MAGIC     - Examples: Age, Tenure, AgeGroup, TenureCategory
# MAGIC     - Stored as columns (not computed views)
# MAGIC     - Avoids repetitive calculation in Gold layer
# MAGIC     - Trade-off: storage vs compute
# MAGIC
# MAGIC 28. **How do you handle NULL values in transformations?**
# MAGIC     - COALESCE for default values
# MAGIC     - CASE statements for conditional logic
# MAGIC     - Separate validation flags (HasValidEmail)
# MAGIC     - Don't drop NULLs, flag for downstream handling
# MAGIC     - Business rules determine NULL treatment
# MAGIC
# MAGIC 29. **What would you do differently if data volume increased 100x?**
# MAGIC     - **Partition tables** by date/region
# MAGIC     - **Auto Loader** instead of REST API polling
# MAGIC     - **Streaming pipelines** for real-time processing
# MAGIC     - **Spark Declarative Pipelines** for orchestration
# MAGIC     - **Photon engine** for faster query execution
# MAGIC     - **Cluster sizing**: Increase workers for parallelism
# MAGIC
# MAGIC 30. **How do you ensure data quality in Silver layer?**
# MAGIC     - Type enforcement via schema registry
# MAGIC     - NOT NULL constraints on key columns
# MAGIC     - Range checks (age > 0, dates in valid range)
# MAGIC     - Referential integrity (foreign key validation)
# MAGIC     - Duplicate detection via hash keys
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Gold Layer - Analytics (31-40)
# MAGIC
# MAGIC 31. **What business metrics did you create in the Gold layer?**
# MAGIC     - Monthly revenue and claims
# MAGIC     - Customer acquisition and churn
# MAGIC     - Loss ratio (claims paid / premiums collected)
# MAGIC     - Revenue per customer
# MAGIC     - MoM and YoY growth rates
# MAGIC     - Policy profitability by type
# MAGIC     - Regional performance metrics
# MAGIC     - Fraud detection indicators
# MAGIC
# MAGIC 32. **Explain the window functions used for growth calculations.**
# MAGIC     - `LAG(TotalRevenue, 1) OVER (ORDER BY Year, Month)` → Previous month
# MAGIC     - `LAG(TotalRevenue, 12) OVER (ORDER BY Year, Month)` → Previous year
# MAGIC     - Calculate percentage change for MoM and YoY
# MAGIC     - NULL handling for first periods
# MAGIC
# MAGIC 33. **How did you optimize Gold layer aggregations?**
# MAGIC     - **Pre-aggregated tables** instead of views
# MAGIC     - **Materialized results** for faster dashboard queries
# MAGIC     - **Partition by time** (year, month) for time-series queries
# MAGIC     - **Overwrite mode** for full refresh (simpler than incremental)
# MAGIC     - **Compact schema** with only necessary columns
# MAGIC
# MAGIC 34. **What would you change if dashboards needed real-time data?**
# MAGIC     - **Streaming aggregations** with watermarking
# MAGIC     - **Incremental Gold refresh** (not full overwrite)
# MAGIC     - **Lower latency sources** (Kafka instead of REST API)
# MAGIC     - **Delta Live Tables** for continuous updates
# MAGIC     - **Caching** frequently queried aggregates
# MAGIC
# MAGIC 35. **How do you handle late-arriving data in Gold layer?**
# MAGIC     - Include processing date separate from event date
# MAGIC     - Recalculate affected time periods on refresh
# MAGIC     - Use event timestamp for business logic
# MAGIC     - Watermark strategy for streaming (if applicable)
# MAGIC
# MAGIC 36. **Explain the loss ratio calculation and its business importance.**
# MAGIC     - Loss Ratio = (Total Claims Paid / Total Premiums) × 100
# MAGIC     - Key profitability indicator for insurance
# MAGIC     - Target: < 70% for healthy business
# MAGIC     - High ratio → underpriced policies or fraud
# MAGIC     - Calculated monthly for trend analysis
# MAGIC
# MAGIC 37. **How would you implement real-time fraud detection?**
# MAGIC     - **ML model** trained on historical fraud patterns
# MAGIC     - **Feature engineering**: claim amount, processing speed, customer history
# MAGIC     - **Streaming scoring** on incoming claims
# MAGIC     - **Alert system** for high-risk claims
# MAGIC     - **Feedback loop** to retrain model
# MAGIC
# MAGIC 38. **What BI tools would you connect to Gold layer?**
# MAGIC     - Databricks SQL Warehouse for querying
# MAGIC     - Power BI or Tableau for dashboards
# MAGIC     - Lakeview for embedded analytics
# MAGIC     - Python notebooks for ad-hoc analysis
# MAGIC     - REST APIs for custom applications
# MAGIC
# MAGIC 39. **How do you optimize query performance for dashboards?**
# MAGIC     - **Z-order** on frequently filtered columns
# MAGIC     - **Liquid clustering** for auto-optimization
# MAGIC     - **Caching** query results in BI tool
# MAGIC     - **Summary tables** at different grain levels
# MAGIC     - **Indexed columns** for fast lookups
# MAGIC     - **Query result caching** in Databricks SQL
# MAGIC
# MAGIC 40. **What additional Gold layer metrics would you add?**
# MAGIC     - Customer lifetime value (CLV) prediction
# MAGIC     - Churn probability scoring
# MAGIC     - Agent performance rankings
# MAGIC     - Product recommendation engine
# MAGIC     - Claim processing efficiency
# MAGIC     - Channel effectiveness (online vs agent vs branch)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Performance Optimization (41-50)
# MAGIC
# MAGIC 41. **What Spark optimizations did you implement?**
# MAGIC     - Broadcast joins for small lookup tables
# MAGIC     - Repartition DataFrames to match cluster size
# MAGIC     - Avoid wide transformations where possible
# MAGIC     - Persist intermediate results when reused
# MAGIC     - Column pruning and predicate pushdown
# MAGIC
# MAGIC 42. **How would you optimize file sizes in Delta tables?**
# MAGIC     - Run `OPTIMIZE table_name` to compact small files
# MAGIC     - Target 128MB-1GB file sizes
# MAGIC     - Use `ZORDER BY` on filter columns
# MAGIC     - Auto-optimize with table properties
# MAGIC     - Vacuum old files after retention period
# MAGIC
# MAGIC 43. **Explain partitioning strategy for this project.**
# MAGIC     - **Bronze**: Minimal partitioning (batch ingestion)
# MAGIC     - **Silver**: Partition by signup_year or state (depending on query patterns)
# MAGIC     - **Gold**: Partition by year, month for time-series queries
# MAGIC     - Avoid over-partitioning (< 1GB per partition)
# MAGIC
# MAGIC 44. **How would you reduce API costs in Bronze layer?**
# MAGIC     - Incremental ingestion with watermarks
# MAGIC     - Cache responses for development/testing
# MAGIC     - Compress API payloads
# MAGIC     - Batch multiple requests
# MAGIC     - Request only changed records
# MAGIC
# MAGIC 45. **What caching strategies would you implement?**
# MAGIC     - `.cache()` on DataFrames used multiple times
# MAGIC     - Disk caching for larger DataFrames
# MAGIC     - Unpersist when no longer needed
# MAGIC     - Cache Gold tables in SQL Warehouse
# MAGIC     - Result caching in dashboards
# MAGIC
# MAGIC 46. **How would you implement autoscaling for this pipeline?**
# MAGIC     - Use **Jobs cluster** with autoscaling enabled
# MAGIC     - Define min/max workers based on load
# MAGIC     - Serverless SQL for Gold layer queries
# MAGIC     - Photon for automatic optimization
# MAGIC     - Schedule off-peak processing
# MAGIC
# MAGIC 47. **What monitoring and alerting would you add?**
# MAGIC     - Pipeline run duration trends
# MAGIC     - Data quality score over time
# MAGIC     - Row count anomaly detection
# MAGIC     - Failed job notifications (email/Slack)
# MAGIC     - Cost per pipeline run tracking
# MAGIC     - SLA compliance metrics
# MAGIC
# MAGIC 48. **How would you implement CI/CD for this project?**
# MAGIC     - Git integration for version control
# MAGIC     - Databricks Asset Bundles for deployment
# MAGIC     - Separate environments (dev, staging, prod)
# MAGIC     - Automated testing with pytest
# MAGIC     - GitHub Actions or Azure DevOps pipelines
# MAGIC
# MAGIC 49. **What security measures would you implement?**
# MAGIC     - Unity Catalog for access control
# MAGIC     - Row-level security on sensitive tables
# MAGIC     - Column masking for PII (email, phone)
# MAGIC     - Secret scopes for API credentials
# MAGIC     - Audit logging for compliance
# MAGIC
# MAGIC 50. **How would you measure the success of this pipeline?**
# MAGIC     - **Technical KPIs**: Uptime, latency, data quality score
# MAGIC     - **Business KPIs**: Faster insights, reduced manual work, cost savings
# MAGIC     - **User adoption**: Dashboard usage, query frequency
# MAGIC     - **Data freshness**: Time from source to Gold layer
# MAGIC     - **ROI**: Cost vs business value delivered

# COMMAND ----------

# MAGIC %md
# MAGIC ## Catalog Metadata Interview Questions - insureallBI
# MAGIC
# MAGIC ### Q1: How many tables are in your insureallBI catalog?
# MAGIC
# MAGIC **Total: 24 tables across 4 schemas**
# MAGIC
# MAGIC * **Bronze Layer (8 tables):** 
# MAGIC   - insurance_customers, insurance_policies, insurance_claims, insurance_payments
# MAGIC   - insurance_agents, countries, customer_policies, payment_frequency
# MAGIC
# MAGIC * **Silver Layer (8 tables):**
# MAGIC   - dimcustomer, dimpolicies, dimagent, dimcountries
# MAGIC   - dimcustomerpolicies, dimpaymentfrequency, factpayments, factclaims
# MAGIC
# MAGIC * **Gold Layer (7 tables):**
# MAGIC   - MonthlyBusinessMetrics, PolicyProfitability, RegionalPerformance
# MAGIC   - PaymentTrends, CustomerLifetimeValue, ClaimsTrends, BusinessHealthMetrics
# MAGIC
# MAGIC * **Logs Layer (1 table):**
# MAGIC   - pipelineruns
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Q2: What SQL queries show table metadata?
# MAGIC
# MAGIC ```sql
# MAGIC -- List all tables
# MAGIC SHOW TABLES IN insureallBI.bronze;
# MAGIC
# MAGIC -- Get table metadata
# MAGIC SELECT table_schema, table_name, table_type
# MAGIC FROM insureallBI.information_schema.tables
# MAGIC WHERE table_schema IN ('bronze', 'silver', 'gold');
# MAGIC
# MAGIC -- Get column counts per table
# MAGIC SELECT table_name, COUNT(*) as ColumnCount
# MAGIC FROM insureallBI.information_schema.columns
# MAGIC WHERE table_schema = 'bronze'
# MAGIC GROUP BY table_name;

# COMMAND ----------

# Get row counts across all layers
schemas = ['bronze', 'silver', 'gold']
for schema in schemas:
    tables = spark.sql(f"SHOW TABLES IN insureallBI.{schema}").collect()
    for row in tables:
        count = spark.table(f"insureallBI.{schema}.{row.tableName}").count()
        print(f"{schema}.{row.tableName}: {count:,} rows")
payemntscount = spark.table(f"insureallasqlcatalog.ins.payments").count()
# print(f"{insureallasqlcatalog}.{ins.payments}: {payemntscount:,} rows")

# COMMAND ----------


## Get sizes for all tables
detail = spark.sql("DESCRIBE DETAIL insureallBI.bronze.insurance_customers").first()
size_gb = detail.sizeInBytes / (1024**3)
print(f"Size: {size_gb:.2f} GB")
print(f"Files: {detail.numFiles}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## What are typical column counts?
# MAGIC By Layer:
# MAGIC
# MAGIC - Bronze tables: 15-25 columns (raw API data)
# MAGIC - Silver dimensions: 20-35 columns (derived fields added)
# MAGIC - Silver facts: 10-20 columns (normalized)
# MAGIC - Gold tables: 15-30 columns (aggregated metrics)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Key Metrics for Interviews:
# MAGIC - 24 tables total across bronze/silver/gold
# MAGIC - 8 bronze tables ingesting REST API data
# MAGIC - 8 silver tables (5 dimensions + 2 facts + 1 bridge)
# MAGIC - 7 gold analytical tables
# MAGIC - 500K customers driving millions of transactions
# MAGIC - 15-35 columns per table on average
# MAGIC - 20-40 GB total storage across all layers
# MAGIC - OPTIMIZE + ZORDER for performance
# MAGIC - Unity Catalog for governance
# MAGIC - Delta Lake features: ACID, time travel, MERGE