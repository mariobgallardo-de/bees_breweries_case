#🐝 Bees Breweries Data <br>

##📌 Overview<br>

This project implements an end‑to‑end data ingestion and analytics pipeline for brewery data sourced from the Open Brewery DB API.<br>
The solution follows modern Lakehouse and Medallion Architecture principles, using Azure-native services and Databricks to deliver a scalable, reliable, and maintainable data platform.<br>
The pipeline ingests raw API data into a Bronze layer, applies standardization and data quality rules in the Silver layer, and produces a curated analytical dataset in the Gold layer.<br><br>

##🏗️ High‑Level Architecture
Open Brewery DB API
        |
        v
Azure Data Factory (ADF)
        |
        v
Azure Databricks
   ├── Bronze ingestion
   ├── Silver transformation
   └── Gold aggregation
        |
        v
Azure Data Lake Storage Gen2 (Delta Lake)
        |
        v
Analytics / BI / Consumption
<br><br>
## Architecture Components<br>

####Azure Data Factory (ADF)

- Acts as the orchestration layer.<br>
- Triggers Databricks notebooks in sequence:<br>
>Bronze ingestion<br>
>Silver transformation<br>
>Gold aggregation<br>


- Centralizes scheduling, retries, and dependency management.<br>

####Azure Databricks

- Core processing and transformation engine.
- Responsible for:

>>API ingestion logic<br>
>Data standardization<br>
>Data Quality validation<br>
>Incremental processing<br>
>Aggregations and analytics-ready outputs<br>


- Delta Lake is used as the storage format for Silver and Gold layers.
<br>

####Azure Data Lake Storage Gen2 (ADLS)

- Central storage layer for the platform.
- Stores:

>>Raw JSON files (Bronze)<br>
>Delta tables (Silver and Gold)


- Data is organized by ingestion date and run ID to support:

>>Auditability<br>
Idempotency<br>
Safe reprocessing
<br>

####Azure Key Vault

- Secure storage of secrets and credentials.
- Integrated with:

>>Azure Data Factory<br>
Databricks secret scopes


- Prevents credentials from being exposed in notebooks or pipelines.
<br>

#### Azure Logic Apps (Alerting)

- Provides event-driven email notifications.
- Triggered on pipeline or job failures.
- Enables proactive operational monitoring without manual intervention.

<br>

##🧱 Data Layers
####🥉 Bronze Layer – Raw Ingestion<br>
Purpose:
Persist raw API responses exactly as received, without transformations.<br>
Key characteristics:<br>
- Page-based ingestion from the Open Brewery DB API.<br>
- Stored as JSON files.<br>

Folder structure:<br>
ingestion_date=YYYY-MM-DD/<br>
run_id=YYYYMMDD_HHMMSS/
    breweries_page_*.json
    _SUCCESS


Marker files (_SUCCESS, _LATEST) are used to guarantee:
- Idempotent ingestion<br>
- Controlled reprocessing<br>
- Support for multiple runs per day if required<br>


####🥈 Silver Layer – Standardized & Validated
Purpose:
Clean, normalize, and enrich data while enforcing quality rules.<br>
Main transformations:
- Text normalization (trim, capitalization).<br>
- Canonical address resolution.<br>
- Latitude and longitude normalization.<br>
- Phone number standardization.<br>
- Deterministic hash generation for incremental updates.

Data Quality flags:
- has_proper_encoding<br>
- has_address<br>
- has_geolocation<br>
- has_website

Incremental strategy:
- Hash-based Delta MERGE.<br>
- Only changed records are updated.

Fail-fast validations:
- Empty dataset detection.<br>
- Duplicate primary keys.<br>
- Threshold-based data quality checks.<br>

####🥇 Gold Layer – Analytical Aggregation
Purpose:
Provide a business-ready dataset for analytics and reporting.<br>
Business definition:
Includes only records that:<br>
- Have proper encoding<br>
- Contain a valid address<br>

Aggregated by:
- Country<br>
- State<br>
- City<br>
- Brewery type<br>

Metrics:
- brewery_count (distinct breweries)<br>
- last_update timestamp<br>

Validations:
- Dataset must not be empty.<br>
- Aggregated counts must be positive.<br>
- Null grouping keys are logged as warnings for monitoring purposes.<br><br>


##📂 Repository Structure
databricks/
  sl_breweries/
    setup/
      └── Environment and table setup notebooks
    utils/
      ├── common              # filesystem helpers and markers
      ├── bronze_lib          # API ingestion logic
      ├── silver_transform    # transformations and DQ
      ├── dq                  # reusable data quality rules
      └── gold_lib            # gold aggregation logic
    executions/
      ├── 01_bronze_execution
      ├── 02_silver_execution
      └── 03_gold_execution

Design principles:
- utils/ contains reusable, modular logic.<br>
- executions/ acts as pipeline entrypoints.<br>
- Clear separation between orchestration and transformation logic.<br><br>


##⚠️ Environment Constraints (Trial Subscription)
This solution was implemented using an Azure Trial Subscription, which introduces specific platform limitations.<br>
Architectural decisions were consciously adapted to remain production‑realistic while ensuring reproducibility.

####⚙️ Compute Strategy

Job Clusters were initially designed for execution.<br>
However, due to compute capacity constraints commonly observed in trial subscriptions, job clusters could not reliably start.

Therefore:
✅ An existing interactive cluster is used for orchestration execution.
This approach:

Does not impact pipeline logic.<br>
Closely mirrors development and lower‑environment patterns frequently used in enterprise setups.<br>


####🗃️ Metastore Persistence

The default Hive Metastore in Databricks Standard workspaces does not persist metadata reliably in this environment.

To ensure reproducibility:

A setup notebook is executed at pipeline start.
Databases and tables are created using IF NOT EXISTS.

This guarantees consistent execution even when metadata is lost between sessions.

####🧭 Why Unity Catalog Was Not Used

Unity Catalog would normally be the recommended governance solution.<br>
Trial account restrictions prevented assigning external metastore permissions at the account level.

Design decision:

External metastores were evaluated but intentionally avoided.<br>
The goal was to keep the solution self-contained, reproducible, and aligned with trial constraints.<br><br>


##✅ Engineering Practices Demonstrated

Medallion Architecture (Bronze / Silver / Gold)<br>
Incremental processing with Delta MERGE<br>
Idempotent ingestion patterns<br>
Secure credential management with Key Vault<br>
Runtime data quality validation<br>
Centralized orchestration via ADF<br>
Event-driven alerting with Logic Apps<br>
Infrastructure-aware design decisions<br><br>


##🧪 Testing Strategy (Future Work)
Due to time constraints and environment limitations, automated unit tests were not fully implemented.<br>
Key considerations:
- Critical data validations are enforced directly in the pipeline runtime.<br>
- Core transformation logic was designed to be testable.<br>
- In a production environment, this logic would be covered by PySpark unit tests executed in CI.<br><br>


##🚀 Final Notes
This project demonstrates:
- Practical Lakehouse architecture design<br>
- Strong understanding of Azure data services<br>
- Emphasis on reliability, data quality, and operational robustness<br>
- Conscious trade-offs aligned with real-world delivery constraints<br>