# Databricks notebook source
# DBTITLE 1,md_gold
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS md_gold
# MAGIC LOCATION 'abfss://gold@stgbeesbreweriescasedev.dfs.core.windows.net/openbrewerydb/';

# COMMAND ----------

# DBTITLE 1,tb_agg_brewery_location
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS md_gold.tb_agg_brewery_location (
# MAGIC     country         STRING        COMMENT 'Country of brewery location',
# MAGIC     state           STRING        COMMENT 'State or province',
# MAGIC     city            STRING        COMMENT 'City name',
# MAGIC     brewery_type    STRING        COMMENT 'Brewery classification',
# MAGIC     brewery_count   BIGINT        COMMENT 'Number of breweries per location and type',
# MAGIC     last_update     TIMESTAMP     COMMENT 'Aggregation execution timestamp'
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://gold@stgbeesbreweriescasedev.dfs.core.windows.net/openbrewerydb/tb_agg_brewery_location'
# MAGIC COMMENT 'Gold aggregated dataset containing brewery distribution by location and type.'
# MAGIC TBLPROPERTIES (
# MAGIC   'Update Frequency' = 'Daily',
# MAGIC   'Source Project' = 'BEES Data Engineering Case',
# MAGIC   'Data Engineer' = 'mariobgallardo@gmail.com',
# MAGIC   'Sensitive Data Indicator' = 'No',
# MAGIC   'Requires Anonymization or Masking' = 'No',
# MAGIC   'Load Type' = 'Full Refresh'
# MAGIC );