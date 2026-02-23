# Databricks notebook source
from pyspark.sql.types import *
import pyspark.sql.functions as F
from delta.tables import DeltaTable


# COMMAND ----------

# MAGIC %md
# MAGIC # Functions

# COMMAND ----------

# DBTITLE 1,Function: Validate Flag Threshold
def validate_flag_threshold(df, flag_column: str, tolerance: float):
    """
    Validates percentage of FALSE values for a given data quality flag.

    Args:
        df: dataframe to validate
        flag_column: boolean flag column name
        tolerance: max allowed percentage (0–1)

    Raises:
        Exception if threshold exceeded
    """

    metrics = (df.select(
            F.count("*").alias("total"),
            F.sum(F.when(F.col(flag_column) == False, 1).otherwise(0))
            .alias("invalid")
        )
        .collect()[0]
    )

    total = metrics["total"]
    invalid = metrics["invalid"]

    ratio = invalid / total if total > 0 else 0

    print(f"[DQ CHECK] - Do NOT {flag_column}: {invalid}/{total} ({ratio:.2%})")

    if ratio > tolerance:
        raise Exception(
            f"SILVER DATA ALERT → {flag_column} exceeded tolerance "
            f"({ratio:.2%} > {tolerance:.2%})"
        )

# COMMAND ----------

# MAGIC %md
# MAGIC #Usage

# COMMAND ----------

# DBTITLE 1,Load raw JSON data
bronze_base_path = "abfss://bronze@stgbeesbreweriescasedev.dfs.core.windows.net/openbrewerydb"

# Check latest extracted JSON file
folders = dbutils.fs.ls(bronze_base_path)

ingestion_dates = [
    f.name.replace("ingestion_date=", "").replace("/", "")
    for f in folders
    if f.name.startswith("ingestion_date=")
]

latest_ingestion = max(ingestion_dates)
print(f"Latest ingestion detected: {latest_ingestion}")

# Load latest JSON file
latest_path = f"{bronze_base_path}/ingestion_date={latest_ingestion}"

df_raw = (
    spark.read
        .option("multiLine", True)
        .json(latest_path)
)

# COMMAND ----------

# DBTITLE 1,Data Standarization
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

df_standardized = df_raw\
    .withColumn("name", F.trim(F.col("name")))\
    .withColumn("country", F.initcap(F.trim(F.col("country"))))\
    .withColumn("state", F.initcap(F.trim(F.col("state"))))\
    .withColumn("city", F.initcap(F.trim(F.col("city"))))\
    .withColumn("address",F.concat_ws(", ", F.col("address_1"), F.col("address_2"), F.col("address_3")))\
    .withColumn("phone", F.regexp_replace(F.trim(F.col("phone")), r"[^\d+]", ""))\
    .withColumn("latitude", F.round(F.col("latitude"), 6))\
    .withColumn("longitude", F.round(F.col("longitude"), 6))\
    .withColumn("ingestion_timestamp", F.current_timestamp()) 



# Create a flag for records with encoding issues and missing values
text_cols = ["name","address","country","city","state","street"]

condition = None

for c in text_cols:
    expr = F.col(c).contains("\uFFFD")
    condition = expr if condition is None else (condition | expr)

df_flagged = df_standardized\
    .withColumn("has_proper_encoding", ~condition)\
    .withColumn("has_website", F.col("website_url").isNotNull())\
    .withColumn("has_address", F.col("address").isNotNull())\
    .withColumn("has_geolocation", F.col("latitude").isNotNull() & F.col("longitude").isNotNull())\


# Create HASH column for Merge
business_columns = [
    "name",
    "brewery_type",
    "address",
    "postal_code",
    "city",
    "state",
    "country",
    "latitude",
    "longitude",
    "phone",
    "website_url",
    "has_proper_encoding",
    "has_geolocation",
    "has_address",
    "has_website"
]

df_hash = df_flagged.withColumn(
    "hash_merge",
    F.sha2(
        F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in business_columns]),
        256
    )
)


# Generate silver DF
df_silver = df_hash.select(
    "id"
    ,"name"
    ,"brewery_type"
    ,"address"
    ,"postal_code"
    ,"city"
    ,"state"
    ,"country"
    ,"latitude"
    ,"longitude"
    ,"phone"
    ,"website_url"
    ,"has_proper_encoding"
    ,"has_geolocation"
    ,"has_address"
    ,"has_website"
    ,"ingestion_timestamp"
    ,"hash_merge"
    )

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

# Dataset is empty:
if df_silver.count() == 0:
    raise Exception("SILVER FAILURE: Transformation produced empty dataset.")

# Duplicated values for Brewery ID:
if df_silver.groupBy("id").count().filter("count > 1").count() > 0:
    raise Exception("SILVER FAILURE: Duplicated values for Brewery ID.")

# Validating Flag Thresholds: 
validate_flag_threshold(df_silver, "has_geolocation", 0.40)
validate_flag_threshold(df_silver, "has_address", 0.05)
validate_flag_threshold(df_silver, "has_website", 0.60)
validate_flag_threshold(df_silver, "has_proper_encoding", 0.01)

# COMMAND ----------

# DBTITLE 1,Save tb_dim_brewery
# Definig target and source tables
delta_table = DeltaTable.forName(spark, "md_silver.tb_dim_brewery")

(
delta_table.alias("t")
.merge(
    df_silver.alias("s"),
    "t.id = s.id"
)
.whenMatchedUpdate(
    condition="t.hash_merge <> s.hash_merge",
    set={
        "id": "s.id",
        "name": "s.name",
        "brewery_type": "s.brewery_type",
        "address": "s.address",
        "postal_code": "s.postal_code",
        "city": "s.city",
        "state": "s.state",
        "country": "s.country",
        "latitude": "s.latitude",
        "longitude": "s.longitude",
        "phone": "s.phone",
        "website_url": "s.website_url",
        "has_proper_encoding": "s.has_proper_encoding",
        "has_geolocation": "s.has_geolocation",
        "has_address": "s.has_address",
        "has_website": "s.has_website",
        "hash_merge": "s.hash_merge",
        "ingestion_timestamp": "s.ingestion_timestamp"
    }
)
.whenNotMatchedInsertAll()
.execute()
)