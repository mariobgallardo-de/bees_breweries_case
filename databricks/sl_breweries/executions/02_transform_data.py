# Databricks notebook source
from pyspark.sql.types import *
import pyspark.sql.functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Utils

# COMMAND ----------

# DBTITLE 1,DQ
# MAGIC %run /Workspace/Repos/mariobgallardo@gmail.com/bees_breweries_case/databricks/sl_breweries/utils/dq

# COMMAND ----------

# DBTITLE 1,Silver Transformations
# MAGIC %run /Workspace/Repos/mariobgallardo@gmail.com/bees_breweries_case/databricks/sl_breweries/utils/silver_transform

# COMMAND ----------

# DBTITLE 1,Common Functions
# MAGIC %run /Workspace/Repos/mariobgallardo@gmail.com/bees_breweries_case/databricks/sl_breweries/utils/common

# COMMAND ----------

# MAGIC %md
# MAGIC ##Configuration

# COMMAND ----------

# DBTITLE 1,Setting parameters
bronze_base_path = "abfss://bronze@stgbeesbreweriescasedev.dfs.core.windows.net/openbrewerydb"

target_table = "md_silver.tb_dim_brewery"

# DQ Thresholds
TOL_GEO = 0.40
TOL_ADDR = 0.05
TOL_WEB  = 0.60
TOL_ENC  = 0.01


# COMMAND ----------

# MAGIC %md
# MAGIC ##Usage

# COMMAND ----------

# DBTITLE 1,Load raw JSON data

latest_ingestion, latest_run_id, latest_run_path = pick_latest_valid_day_and_run(bronze_base_path)

print(f"[INFO] Latest valid ingestion_date: {latest_ingestion}")
print(f"[INFO] Latest valid run_id: {latest_run_id}")
print(f"[INFO] Reading JSON pages from: {latest_run_path}")

df_raw = (
    spark.read
        .option("multiLine", True)
        .json(f"{latest_run_path}/breweries_page_*.json")
)

# COMMAND ----------

# DBTITLE 1,Standarization
"""
    Records containing Unicode replacement characters (U+FFFD, "�") were identified.
    The issue originates from the source API payload, not from the ingestion process.
    A data quality flag was added instead of modifying raw values.

    In addition to data quality flagging, textual attributes were standardized
    to ensure analytical consistency across records. This includes trimming
    whitespace, applying consistent capitalization to location fields,
    normalizing phone number formats, dropping redundant information and creating
    derived columns to support downstream analysis and data quality monitoring.
"""

df_standardized = standardize(df_raw)
df_flagged = add_quality_flags(df_standardized)
df_hash = add_hash(df_flagged)
df_silver = build_silver_df(df_hash)


# COMMAND ----------

# DBTITLE 1,Validation
"""
    This step is responsible for validating the data quality of the silver table.
    
    The validation is performed by checking the bellow conditions:
    - If the dataset is empty;
    - If the dataset has duplicated values for Brewery ID;
    - If the amount of registers with encoding issues is greater than the tolerance threshold;
    - If the amount of registers with geolocation greater than the tolerance threshold;
    - If the amount of registers with address greater than the tolerance threshold;
    - If the amount of registers with website greater than the tolerance threshold.
"""

df_silver.cache()

if df_silver.count() == 0:
    raise Exception("SILVER FAILURE: Transformation produced empty dataset.")

if df_silver.groupBy("id").count().filter("count > 1").count() > 0:
    raise Exception("SILVER FAILURE: Duplicated values for Brewery ID.")

validate_flag_threshold(df_silver, "has_geolocation", TOL_GEO)
validate_flag_threshold(df_silver, "has_address", TOL_ADDR)
validate_flag_threshold(df_silver, "has_website", TOL_WEB)
validate_flag_threshold(df_silver, "has_proper_encoding", TOL_ENC)


# COMMAND ----------

# DBTITLE 1,Save tb_dim_brewery

delta_table = DeltaTable.forName(spark, target_table)

(
    delta_table.alias("t")
    .merge(df_silver.alias("s"), "t.id = s.id")
    .whenMatchedUpdate(
        condition="t.hash_merge <> s.hash_merge",
        set={c: f"s.{c}" for c in df_silver.columns}  # atualiza tudo do source
    )
    .whenNotMatchedInsertAll()
    .execute()
)

df_silver.unpersist()

print("[SUCCESS] Silver layer updated.")

