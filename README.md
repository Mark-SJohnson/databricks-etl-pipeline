# Azure Databricks Medallion Architecture Pipeline

## Overview
This project implements a medallion architecture (bronze → silver → gold) data pipeline using Azure Databricks, demonstrating end-to-end data engineering practices including ingestion, transformation, governance, performance optimization, orchestration, and secure authentication. Built as hands-on preparation for the DP-750: Implementing Data Engineering Solutions Using Azure Databricks certification.

## Objectives
- Provision and configure an Azure Databricks workspace and Serverless compute
- Organize data using Unity Catalog (catalogs, schemas, tables)
- Design a bronze/silver/gold layered data architecture using Delta Lake
- Ingest and transform data using PySpark and Spark SQL
- Apply security and governance controls at the table/column/row level
- Apply performance optimization techniques (partitioning, Z-Ordering, OPTIMIZE, VACUUM)
- Orchestrate the pipeline as an automated, dependency-aware job
- Configure service principal authentication for non-interactive access
- Document real design decisions, errors encountered, and trade-offs

## Architecture

```
Sample Source Data (samples.nyctaxi.trips)
      │
      ▼
┌─────────────┐   PySpark (DataFrame write)
│   Bronze    │   Raw data, registered as a Delta table
└─────────────┘
      │
      ▼
┌─────────────┐   PySpark (filtering, derived columns)
│   Silver    │   Cleaned, validated, conformed schema
└─────────────┘
      │
      ▼
┌─────────────┐   Spark SQL (groupBy aggregation)
│    Gold     │   Business-level daily/zip-level summary
└─────────────┘
      │
      ▼
  Orchestrated via Databricks Jobs (Bronze → Silver → Gold)
```

## Environment Setup
1. Provisioned an Azure Databricks workspace (Premium tier, Hybrid workspace type) on a 14-day trial subscription
2. Used Serverless compute for all notebook execution and job tasks
3. Unity Catalog was auto-provisioned with the workspace; created a dedicated catalog (`dbw_dp750_lab`) and schema (`medallion_project`) for this project
4. Verified catalog/schema structure via the Catalog UI and confirmed with SQL queries when the UI lagged behind actual state

## Implementation Steps

### 1. Bronze Layer — Raw Ingestion
- Read the built-in Databricks sample dataset `samples.nyctaxi.trips` using PySpark
- Wrote it as a Delta table (`bronze_taxi_trips`) registered in Unity Catalog, preserved as-is for auditability
- Verified via both a SQL query and the Catalog UI (encountered and resolved a UI lag issue — see Lessons Learned)

### 2. Silver Layer — Cleansing & Conformance
- Used a PySpark notebook to:
  - Filter out invalid records: non-positive fares, zero/negative trip distances, and dropoff timestamps not after pickup timestamps
  - Add a derived column, `trip_duration_minutes`, calculated from the pickup/dropoff timestamps
  - Remove exact duplicate rows
- Discovered and fixed a real data quality issue: an initial pass let through a 1.46-mile, $8.00 trip with a ~22.6 hour duration, since the original filter only checked direction (dropoff after pickup), not a reasonable range. Added a `trip_duration_minutes <= 180` cap to catch this class of error (see Lessons Learned for full detail)
- Row count dropped from 21,932 (bronze) to 21,847 (silver) after cleaning

### 3. Gold Layer — Business-Ready Modeling
- Used Spark SQL (`groupBy`/`agg`) to build a daily, per-pickup-zip summary table: total trips, average fare, average trip duration, and average distance
- Produced 3,271 aggregated rows from the 21,847 silver-layer records

### 4. Security & Governance (Unity Catalog)
- Created a SQL function (`mask_fare`) using `is_account_group_member()` to conditionally mask the `avg_fare` column in the gold table — full values for members of a `finance_analyst` group, values rounded to the nearest 10 for everyone else. Applied via `ALTER TABLE ... ALTER COLUMN ... SET MASK`.
- Created a second SQL function (`zip_row_filter`) and applied it via `ALTER TABLE ... SET ROW FILTER` to restrict which rows of the gold table are visible based on group membership (`northeast_team` or `admins`).
- **Observed and verified real row-filter behavior**: with no matching group membership on the account, a `SELECT *` against the filtered table correctly returned zero rows — direct, working proof that Unity Catalog silently excludes rows a user isn't authorized to see, rather than erroring or exposing them.
- Column masking behavior could not be directly observed end-to-end in this single-account lab environment, since account admins typically bypass masks by default; the mechanism is documented and correctly configured, but full before/after masked-value testing would require a second, non-privileged user account in a multi-user workspace.
- Removed the row filter afterward (`ALTER TABLE ... DROP ROW FILTER`) to restore full access for continued development.

### 5. Performance Optimization
- **Partitioning**: Repartitioned `gold_daily_zip_summary` by `trip_date` using `.partitionBy("trip_date")` with `overwriteSchema` enabled (required since partitioning changes table structure, which Delta blocks by default during overwrite). Result: 60 files, one per unique date, ~172KB total.
- **Z-Ordering**: Ran `OPTIMIZE ... ZORDER BY (pickup_zip)`. Result: 0 files added/removed, since each date partition already contained only a single small file — there was nothing to compact or reorder. This confirms OPTIMIZE/Z-Ordering deliver their real value on tables with many files per partition (e.g., from frequent small writes or streaming), not on small, already-compact tables like this one.
- **VACUUM**: Ran successfully against the table; no files were eligible for deletion, as expected for a table with no file history older than the default 7-day retention window.
- **Caching — architectural limitation discovered**: Attempted to demonstrate caching using both the SQL `CACHE TABLE` command and the DataFrame `.cache()` method. Both failed with `NOT_SUPPORTED_WITH_SERVERLESS` errors. This revealed a genuine architectural constraint: Serverless compute's stateless, elastic design does not support persistent in-memory caching (which relies on a dedicated, long-running cluster to hold data in memory). In a production scenario requiring caching for repeated query patterns, classic compute (a dedicated cluster) would be the correct choice over Serverless — a real example of a compute-type tradeoff decision.

### 6. Deployment & Orchestration
- Created a Databricks Job (`Medallion_pipeline_dp750`) using Jobs & Pipelines to orchestrate all three notebooks as a single automated pipeline
- Configured task dependencies so each stage only runs after the previous one succeeds: `bronze_ingestion` → `silver_transformation` → `gold_aggregation`
- All tasks run on Serverless compute
- **Verified with a full end-to-end run**: all three tasks succeeded in sequence (Bronze: 49s, Silver: 20s, Gold: 59s), confirming the pipeline runs correctly as an orchestrated job rather than requiring manual notebook-by-notebook execution
- Job run history is viewable via the Runs tab, providing a natural audit trail for each pipeline execution

### 7. Security — Service Principal Authentication
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
- Delta Lake fundamentals: ACID transactions, schema enforcement
- PySpark and Spark SQL transformations at scale

## Lessons Learned
- **DataFrame display requires explicit `.show()`**: Running a Spark SQL query without chaining `.show()` at the end only prints the DataFrame's schema/structure (e.g., `DataFrame[col: type, ...]`), not the actual row data. Easy to miss when first getting oriented in notebooks — always confirm output with `.show()` when you expect to see rows.
- **Unity Catalog browser UI can lag behind actual state**: After creating a new table via a notebook (e.g., `saveAsTable`), the Catalog explorer panel may still show "No data" for the containing schema even though the table exists and is fully queryable via SQL. A manual refresh (refresh icon in the Catalog panel) or re-navigating into the schema resolves it. Worth verifying via a SQL query first if the UI seems out of sync, rather than assuming the write failed.
- **Timestamp range validation needs sanity bounds, not just direction checks**: An initial silver-layer filter of `dropoff_datetime > pickup_datetime` correctly removes trips with backwards or equal timestamps, but doesn't catch records where the gap is technically positive yet physically implausible. One record in the sample data showed a 1.46-mile, $8.00 trip with a 1,358-minute (~22.6 hour) duration — clearly a corrupted timestamp, not a real trip. Fixed by adding an upper-bound filter (`trip_duration_minutes <= 180`) alongside the directional check. Takeaway: data validation on time-based fields should include both a directional check and a reasonable range/sanity check, since real-world data can pass a "positive difference" test while still being nonsensical.
- **Catalog/schema context doesn't reliably persist across cells or languages**: `USE CATALOG` / `USE SCHEMA` statements set in one cell don't always carry over to a later cell, especially when mixing SQL and Python (`%python`) within the same notebook. Switched to using fully qualified `catalog.schema.table` names in every query/write to avoid intermittent `TABLE_OR_VIEW_NOT_FOUND` errors.
- **Serverless compute has real, hard limitations**: Both the SQL `CACHE TABLE` command and the DataFrame `.cache()`/`.persist()` methods fail outright on Serverless compute, since its stateless, elastic architecture doesn't support persistent in-memory caching. This is a genuine compute-type tradeoff to weigh in real design decisions, not just a lab quirk.
- **Unity Catalog `GRANT` statements require IDs, not display names, for service principals**: Attempting to grant permissions using a service principal's friendly Databricks display name failed with `PRINCIPAL_DOES_NOT_EXIST`; the underlying Entra ID Application (client) ID is required instead.

## Next Steps
- Build a second project focused specifically on multi-user Unity Catalog governance (e.g., simulating multiple teams with different access levels) to fully observe column-masking behavior with a non-admin account
- Explore Delta Live Tables / Lakeflow declarative pipelines as an alternative to notebook-based orchestration
- Integrate with Azure Data Factory for broader cross-service orchestration
- Explore Databricks Asset Bundles (DABs) for CI/CD-style deployment

---
*Part of a DP-750 (Azure Databricks Data Engineer Associate) certification preparation series.*