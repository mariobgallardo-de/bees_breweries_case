# Databricks notebook source
# DBTITLE 1,md_silver
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS md_silver
# MAGIC LOCATION 'abfss://silver@stgbeesbreweriescasedev.dfs.core.windows.net/openbrewerydb/';

# COMMAND ----------

# DBTITLE 1,tb_dim_brewery
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS md_silver.tb_dim_brewery (
# MAGIC     id                  STRING      COMMENT     'Unique identifier of the brewery provided by the source API',
# MAGIC     name                STRING      COMMENT     'Commercial name of the brewery location',
# MAGIC     brewery_type        STRING      COMMENT     'Classification of brewery according to the source system (micro, brewpub, large, etc.)',
# MAGIC     address             STRING      COMMENT     'Standardized full address combining address fields from the source',
# MAGIC     postal_code         STRING      COMMENT     'Postal or ZIP code associated with the brewery location',
# MAGIC     city                STRING      COMMENT     'City where the brewery is located (standardized capitalization)',
# MAGIC     state               STRING      COMMENT     'State or province of the brewery location',
# MAGIC     country             STRING      COMMENT     'Country of the brewery location',
# MAGIC     latitude            DOUBLE      COMMENT     'Geographical latitude coordinate rounded to standardized precision',
# MAGIC     longitude           DOUBLE      COMMENT     'Geographical longitude coordinate rounded to standardized precision',
# MAGIC     phone               STRING      COMMENT     'Normalized phone number preserving international prefixes when available',
# MAGIC     website_url         STRING      COMMENT     'Official website URL of the brewery when available',
# MAGIC     has_proper_encoding BOOLEAN     COMMENT     'Indicates presence of Unicode replacement characters (U+FFFD) detected from source data',
# MAGIC     has_geolocation     BOOLEAN     COMMENT     'Flag indicating whether valid latitude and longitude are available',
# MAGIC     has_address         BOOLEAN     COMMENT     'Flag indicating whether address information is present',
# MAGIC     has_website         BOOLEAN     COMMENT     'Flag indicating whether a website URL is available',
# MAGIC     ingestion_timestamp TIMESTAMP   COMMENT     'Timestamp when the record was processed and written into the Silver layer',
# MAGIC     hash_merge          STRING      COMMENT     'UUID based on business columns, used on merge opperation'
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (country, state)
# MAGIC LOCATION 'abfss://silver@stgbeesbreweriescasedev.dfs.core.windows.net/openbrewerydb/tb_dim_brewery'
# MAGIC COMMENT 'Curated dimensional dataset containing standardized brewery entities derived from Open Brewery DB API. The table represents trusted business entities used for analytical aggregation in the Gold layer.'
# MAGIC TBLPROPERTIES (
# MAGIC   'Update Frequency' = 'Daily',
# MAGIC   'Source Project' = 'BEES Data Engineering Case',
# MAGIC   'Data Engineer' = 'mariobgallardo@gmail.com',
# MAGIC   'Sensitive Data Indicator' = 'No',
# MAGIC   'Requires Anonymization or Masking' = 'No',
# MAGIC   'Load Type' = 'Incremental'
# MAGIC )
# MAGIC