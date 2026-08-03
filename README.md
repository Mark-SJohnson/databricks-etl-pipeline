# Azure Databricks Medallion Architecture Pipeline

## Overview
This project implements a medallion architecture (bronze → silver → gold) data pipeline using Azure Databricks, demonstrating end-to-end data engineering practices including ingestion, transformation, governance, and consumption-ready modeling. Built as hands-on preparation for the DP-750: Implementing Data Engineering Solutions Using Azure Databricks certification.

## Objectives
- Provision and configure an Azure Databricks workspace and compute
- Organize data using Unity Catalog (catalogs, schemas, tables)
- Design a bronze/silver/gold layered data architecture using Delta Lake
- Ingest and transform data using PySpark, Spark SQL, and Auto Loader
- Apply security and governance controls at the table/column level
- Apply performance optimization techniques (partitioning, Z-Ordering, caching)
- Document design decisions and trade-offs

## Architecture

```
Raw Source Data
      │
      ▼
┌─────────────┐   Auto Loader / Structured Streaming
│   Bronze    │   Raw, unprocessed, append-only Delta table
└─────────────┘
      │
      ▼
┌─────────────┐   PySpark / Spark SQL notebook
│   Silver    │   Cleaned, validated, conformed schema
└─────────────┘
      │
      ▼
┌─────────────┐   Delta Live Tables / Workflows
│    Gold     │   Business-level aggregates, star schema
└─────────────┘
      │
      ▼
  Power BI / SQL Warehouse consumption
```

## Environment Setup
1. Provisioned an Azure Databricks workspace
2. Configured compute (job compute vs. serverless vs. classic; Photon acceleration)
3. Set up Unity Catalog: created a catalog, schemas, and applied naming conventions
4. Configured Azure Key Vault-backed secrets for secure credential access

## Implementation Steps

### 1. Bronze Layer — Raw Ingestion
- Used Auto Loader to incrementally ingest raw source data (CSV/JSON) from Azure Data Lake Storage Gen2
- Landed data as a Delta table, preserved as-is for auditability and reprocessing

### 2. Silver Layer — Cleansing & Conformance
- Used a PySpark notebook to:
  - Enforce schema and data types
  - Handle nulls, duplicates, and malformed records
  - Write results as validated Delta tables registered in Unity Catalog

### 3. Gold Layer — Business-Ready Modeling
- Built aggregated, star-schema-style tables using Spark SQL / Delta Live Tables
- Optimized for downstream reporting and analytics consumption

### 4. Security & Governance
- Implemented table- and column-level access control in Unity Catalog
- Applied row-level security on gold layer tables
- Authenticated data access using a service principal / managed identity

### 5. Performance Optimization
- Applied Z-Ordering and partitioning strategies on large Delta tables
- Used caching where appropriate for repeated query patterns
- Ran table maintenance (OPTIMIZE, VACUUM) on Delta tables

### 6. Deployment & Monitoring
- Orchestrated the pipeline using Databricks Workflows
- Reviewed job run history and Spark UI for troubleshooting
- Monitored workload health using Azure Monitor integration

### 7. Security & Governance (Unity Catalog)
- Created a SQL function (`mask_fare`) using `is_account_group_member()` to conditionally mask the `avg_fare` column in the gold table — full values for members of a `finance_analyst` group, values rounded to the nearest 10 for everyone else. Applied via `ALTER TABLE ... ALTER COLUMN ... SET MASK`.
- Created a second SQL function (`zip_row_filter`) and applied it via `ALTER TABLE ... SET ROW FILTER` to restrict which rows of the gold table are visible based on group membership (`northeast_team` or `admins`).
- **Observed and verified real row-filter behavior**: with no matching group membership on the account, a `SELECT *` against the filtered table correctly returned zero rows — direct, working proof that Unity Catalog silently excludes rows a user isn't authorized to see, rather than erroring or exposing them.
- Column masking behavior could not be directly observed end-to-end in this single-account lab environment, since account admins typically bypass masks by default; the mechanism is documented and correctly configured, but full before/after masked-value testing would require a second, non-privileged user account in a multi-user workspace.
- Removed the row filter afterward (`ALTER TABLE ... DROP ROW FILTER`) to restore full access for continued development.

### 8. Performance Optimization
- **Partitioning**: Repartitioned `gold_daily_zip_summary` by `trip_date` using `.partitionBy("trip_date")` with `overwriteSchema` enabled (required since partitioning changes table structure, which Delta blocks by default during overwrite). Result: 60 files, one per unique date, ~172KB total.
- **Z-Ordering**: Ran `OPTIMIZE ... ZORDER BY (pickup_zip)`. Result: 0 files added/removed, since each date partition already contained only a single small file — there was nothing to compact or reorder. This confirms OPTIMIZE/Z-Ordering deliver their real value on tables with many files per partition (e.g., from frequent small writes or streaming), not on small, already-compact tables like this one.
- **VACUUM**: Ran successfully against the table; no files were eligible for deletion, as expected for a table with no file history older than the default 7-day retention window.
- **Caching — architectural limitation discovered**: Attempted to demonstrate caching using both the SQL `CACHE TABLE` command and the DataFrame `.cache()` method. Both failed with `NOT_SUPPORTED_WITH_SERVERLESS` errors. This revealed a genuine architectural constraint: Serverless compute's stateless, elastic design does not support persistent in-memory caching (which relies on a dedicated, long-running cluster to hold data in memory). In a production scenario requiring caching for repeated query patterns, classic compute (a dedicated cluster) would be the correct choice over Serverless — a real example of a compute-type tradeoff decision.

### 9. Deployment & Orchestration
- Created a Databricks Job (`Medallion_pipeline_dp750`) using Jobs & Pipelines to orchestrate all three notebooks as a single automated pipeline
- Configured task dependencies so each stage only runs after the previous one succeeds: `bronze_ingestion` → `silver_transformation` → `gold_aggregation`
- All tasks run on Serverless compute
- **Verified with a full end-to-end run**: all three tasks succeeded in sequence (Bronze: 49s, Silver: 20s, Gold: 59s), confirming the pipeline runs correctly as an orchestrated job rather than requiring manual notebook-by-notebook execution
- Job run history is viewable via the Runs tab, providing a natural audit trail for each pipeline execution

### 10. Security — Service Principal Authentication
- Created an app registration in Microsoft Entra ID (`sp-databricks-dp750-lab`) to represent a non-interactive identity for automated/pipeline access, distinct from a personal user login
- Generated a client secret for the service principal to authenticate with
- Registered the service principal in the Databricks workspace (Admin Settings → Identity and access → Service principals), linked via its Entra ID Application (client) ID
- Granted the service principal real permissions using Unity Catalog `GRANT` statements: `USE CATALOG`, `USE SCHEMA`, and `SELECT` on the `medallion_project` schema
- **Key detail discovered**: Unity Catalog `GRANT` statements require the service principal's Application ID (not its Databricks display name) when referencing it — attempting to grant by display name failed with a `PRINCIPAL_DOES_NOT_EXIST` error, since Unity Catalog resolves principals by their underlying identity provider ID
- **Verified with `SHOW GRANTS ON SCHEMA`**: confirmed both `SELECT` and `USE SCHEMA` grants are correctly attached to the service principal's Application ID — real, queryable proof the permission model is working, not just configured

## Key Concepts Demonstrated
- Medallion architecture design principles
- Unity Catalog governance model (catalogs, schemas, access control, lineage)
- Dynamic column masking using SQL functions and `is_account_group_member()`
- Row-level security via row filter functions applied at the table level
- Delta Lake table partitioning and its tradeoffs at small vs. large scale
- OPTIMIZE and Z-Ordering for file compaction and data layout
- VACUUM for managing Delta file retention
- Classic vs. Serverless compute tradeoffs (caching support, cluster persistence)
- Pipeline orchestration and task dependency management using Databricks Jobs
- Service principal authentication via Microsoft Entra ID for non-interactive/automated access
- Delta Lake fundamentals: ACID transactions, schema evolution, time travel
- PySpark and Spark SQL transformations at scale
- Batch vs. streaming ingestion (Auto Loader, Structured Streaming)
- SDLC practices: Git-based version control for notebooks/pipelines

## Lessons Learned
- **DataFrame display requires explicit `.show()`**: Running a Spark SQL query without chaining `.show()` at the end only prints the DataFrame's schema/structure (e.g., `DataFrame[col: type, ...]`), not the actual row data. Easy to miss when first getting oriented in notebooks — always confirm output with `.show()` when you expect to see rows.
- **Unity Catalog browser UI can lag behind actual state**: After creating a new table via a notebook (e.g., `saveAsTable`), the Catalog explorer panel may still show "No data" for the containing schema even though the table exists and is fully queryable via SQL. A manual refresh (refresh icon in the Catalog panel) or re-navigating into the schema resolves it. Worth verifying via a SQL query first if the UI seems out of sync, rather than assuming the write failed.
- **Timestamp range validation needs sanity bounds, not just direction checks**: An initial silver-layer filter of `dropoff_datetime > pickup_datetime` correctly removes trips with backwards or equal timestamps, but doesn't catch records where the gap is technically positive yet physically implausible. One record in the sample data showed a 1.46-mile, $8.00 trip with a 1,358-minute (~22.6 hour) duration — clearly a corrupted timestamp, not a real trip. Fixed by adding an upper-bound filter (`trip_duration_minutes <= 180`) alongside the directional check. Takeaway: data validation on time-based fields should include both a directional check and a reasonable range/sanity check, since real-world data can pass a "positive difference" test while still being nonsensical.
- Document additional friction points here (e.g., schema drift handling, pipeline retry logic, compute throttling behavior observed during testing)

## Next Steps
- Extend pipeline with Delta Live Tables for declarative pipeline management
- Integrate with Azure Data Factory for broader orchestration
- Explore Databricks Asset Bundles for CI/CD deployment

---
*Part of a DP-750 (Azure Databricks Data Engineer Associate) certification preparation series.*


