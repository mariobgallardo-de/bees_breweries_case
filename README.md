# 🐝 Bees Breweries Data Platform

## 📌 Overview

This project implements an end‑to‑end data ingestion and analytics pipeline for brewery data sourced from the **Open Brewery DB API**.

The solution follows modern **Lakehouse** and **Medallion Architecture** principles, using Azure‑native services and Databricks to deliver a scalable, reliable, and maintainable data platform.

The pipeline ingests raw API data into a **Bronze** layer, applies standardization and data quality rules in the **Silver** layer, and produces a curated analytical dataset in the **Gold** layer.

---

## High‑Level Architecture

```text
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
```
---

## Pipeline Orchestration (ADF)

The pipeline is orchestrated using Azure Data Factory, which triggers Databricks notebooks sequentially.

A master Azure Data Factory pipeline orchestrates the end‑to‑end workflow by invoking the pipeline that executes all processing steps (Bronze, Silver and Gold).
On failure, the master pipeline triggers an automated email notification via Azure Logic Apps.

<p align="center">
  <img src="images/adf_pipeline.png" width="800"/>
</p>

---

## Azure Resources

The following Azure resources were provisioned for this solution:

- **Resource Group**
  - `rg-bees-de-case-dev`

- **Azure Data Lake Storage Gen2**
  - Storage account: `stgbeesbreweriescasedev`
  - Containers used: `bronze`, `silver`, `gold`

- **Azure Databricks Workspace**
  - `adb-bees-breweries-case-dev`

- **Azure Data Factory**
  - `adf-bees-breweries-case-dev`
  - Master pipeline orchestrates execution of Bronze → Silver → Gold notebooks

- **Azure Key Vault**
  - `kv-bees-breweries-dev`
  - Stores secrets for Databricks and ADF integration

- **Azure Logic App**
  - `la-bees-alert`
  - Sends email notifications on pipeline failures

---

## Architecture Components

### Azure Data Factory (ADF)

- Acts as the orchestration layer.
- Triggers Databricks notebooks in sequence:
  - Bronze ingestion
  - Silver transformation
  - Gold aggregation
- Centralizes scheduling, retries, and dependency management.

---

### Azure Databricks

- Core processing and transformation engine.
- Responsible for:
  - API ingestion logic
  - Data standardization
  - Data quality validation
  - Incremental processing
  - Aggregations and analytics‑ready outputs
- Delta Lake is used as the storage format for Silver and Gold layers.

---

### Azure Data Lake Storage Gen2 (ADLS)

- Central storage layer for the platform.
- Stores:
  - Raw JSON files (Bronze)
  - Delta tables (Silver and Gold)
- Data is organized by ingestion date and run ID to support:
  - Auditability
  - Idempotency
  - Safe reprocessing

---

### Azure Key Vault

- Secure storage of secrets and credentials.
- Integrated with:
  - Azure Data Factory
  - Databricks secret scopes
- Prevents credentials from being exposed in notebooks or pipelines.

---

### Azure Logic Apps (Alerting)

- Provides event‑driven email notifications.
- Triggered on pipeline or job failures.
- Enables proactive operational monitoring.

---

## Data Layers

### 🥉 Bronze Layer – Raw Ingestion

**Purpose:**  
Persist raw API responses exactly as received, without transformations.

**Notebook:**
databricks/sl_breweries/executions/01_extract_breweries_data

**Key characteristics:**
- Page‑based ingestion from the Open Brewery DB API.
- Stored as JSON files.
- Folder structure:

```text
ingestion_date=YYYY-MM-DD/
  run_id=YYYYMMDD_HHMMSS/
    breweries_page_*.json
    _SUCCESS
  _LATEST
  _SUCCESS
```

Marker files (`_SUCCESS`, `_LATEST`) guarantee:

- **Idempotent ingestion**  
  Raw data is ingested only once per successful run. Re‑executions do not duplicate data.

- **Controlled reprocessing**  
  When data for a given day is already successfully ingested, the Bronze layer safely **skips execution** instead of failing, allowing Silver and Gold layers to be reprocessed independently.

- **Support for multiple runs per day**  
  When enabled, multiple ingestion runs can occur within the same day, each identified by a unique `run_id`.

**Lifecycle Management:**
A storage‑level lifecycle policy is applied to the `bronze/openbrewerydb/` prefix, automatically deleting Bronze data older than 30 days.

---

### 🥈 Silver Layer — Standardized & Validated

**Purpose:**  
Produce a curated **single source of truth** for brewery data, applying standardization and enforcing data quality rules.

**Notebook:**
databricks/sl_breweries/executions/02_transform_data

**Transformations (high level):**
- Text normalization (trim and capitalization)
- Canonical address resolution
- Latitude and longitude casting and rounding
- Phone number normalization
- Deterministic hash generation for incremental processing

**Output table:** `md_silver.tb_dim_brewery`  
**Primary key:** `id`

**Core columns (selected):**
- `name`, `brewery_type`
- `address`, `postal_code`, `city`, `state`, `country`
- `latitude`, `longitude`
- `phone`, `website_url`
- `ingestion_timestamp`
- `hash_merge` (used for incremental `MERGE`)

**Data Quality flags:**
- `has_proper_encoding`
- `has_address`
- `has_geolocation`
- `has_website`

**DQ validations (fail‑fast):**
- Empty dataset detection
- Duplicate primary key detection
- Threshold‑based checks on quality flags
---

### 🥇 Gold Layer — Analytical Aggregation

**Purpose:**  
Provide an analytics‑ready dataset optimized for reporting and consumption.

**Notebook:**
databricks/sl_breweries/executions/03_aggregate_data

**Business rules:**
- Only records with:
  - `has_proper_encoding = true`
  - `has_address = true`
  are included in the aggregation.

**Output table:** `md_gold.tb_agg_brewery_location`  
**Grain:** One row per `(country, state, city, brewery_type)`

**Metrics:**
- `brewery_count` — number of distinct breweries
- `last_update` — aggregation timestamp

**DQ validations:**
- Non‑empty aggregation (fail‑fast)
- `brewery_count > 0` (fail‑fast)
- Null grouping keys logged as warnings (monitoring)

---

## Repository Structure

```text
databricks/
  sl_breweries/
    setup/
      ├── 01_silver_breweries
      └── 02_gold_breweries
    utils/
      ├── common            # filesystem helpers and markers
      ├── bronze_lib        # API ingestion logic
      ├── silver_transform  # transformations and reading JSON
      ├── dq                # reusable data quality rules
      └── gold_lib          # gold aggregation logic
    executions/
      ├── 01_extract_breweries_data (Bronze)
      ├── 02_transform_data (Silver)
      └── 03_aggregate_data (Gold)
```

**Design principles:**
- `utils/` contains reusable, modular logic
- `executions/` act as pipeline entrypoints
- Clear separation between orchestration and transformation logic

---

## Source Control and Versioning

This project follows enterprise‑grade source control practices, with all orchestration and processing logic versioned in GitHub.

### Databricks

- All Databricks notebooks are versioned using **Databricks Repos**.
- This ensures:
  - Full change history and traceability
  - Reproducibility of data pipelines
  - Alignment with collaborative development workflows

### Azure Data Factory (ADF)

- Azure Data Factory is connected to the same GitHub repository in **Git mode**.
- Pipelines, datasets, and linked services are versioned in the collaboration branch.
- Changes are committed to the collaboration branch (e.g. `main`) while working in draft mode.

---

## Security & Identity

Authentication and secret management follow secure practices:

- A **Microsoft Entra ID Service Principal** is used for OAuth-based access (IDs not disclosed).
- All secrets (tokens, credentials, endpoints) are stored in **Azure Key Vault** and referenced via Databricks secret scopes.
- No secrets are stored in notebooks, ADF pipelines, or the public repository.

---

## ⚠️ Environment Constraints (Trial Subscription)

This solution was implemented using an **Azure Trial Subscription**, which introduces specific platform limitations.

Architectural decisions were consciously adapted to remain **production‑realistic while ensuring reproducibility**.

---

### Compute Strategy

- Job Clusters were initially designed for execution.
- Due to compute capacity constraints commonly observed in trial subscriptions, job clusters could not reliably start.

Therefore, an existing **interactive cluster** is used for orchestration execution.

This approach:
- Does not impact pipeline logic
- Mirrors development and lower‑environment enterprise patterns

---

### Metastore Persistence

- The default Hive Metastore in Databricks Standard workspaces does not persist metadata reliably.

**Mitigation strategy:**
- Setup notebooks runs at pipeline start
  - databricks/sl_breweries/setup/01_silver_breweries
  - databricks/sl_breweries/setup/02_gold_breweries  
- Databases and tables are created using `IF NOT EXISTS`

This guarantees consistent execution even when metadata is lost between sessions.

---

### Why Unity Catalog Was Not Used

- Unity Catalog would normally be the recommended governance solution.
- Trial account restrictions prevented assigning external metastore permissions.

**Design decision:**
- External metastores were evaluated but intentionally avoided
- The solution remains self‑contained and reproducible

---

## Testing Strategy (Future Work)

Due to time constraints and environment limitations, automated unit tests were not fully implemented.

- Critical data validations are enforced directly in the pipeline runtime
- Core transformation logic was designed to be testable
- In production, this logic would be covered by PySpark unit tests executed in CI

---

## Final Notes

Engineering Practices Applied:
- Medallion Architecture (Bronze / Silver / Gold)
- Incremental processing with Delta `MERGE`
- Idempotent ingestion patterns
- Secure credential management with Key Vault
- Runtime data quality validation
- Centralized orchestration via ADF
- Event‑driven alerting with Logic Apps
- Infrastructure‑aware design decisions
