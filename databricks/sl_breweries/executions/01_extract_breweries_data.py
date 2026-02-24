# Databricks notebook source
# DBTITLE 1,Imports
import requests
import json
import time
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Utils

# COMMAND ----------

# DBTITLE 1,Bronze Extract API Functions
# MAGIC %run /Workspace/Repos/mariobgallardo@gmail.com/bees_breweries_case/databricks/sl_breweries/utils/bronze_selection

# COMMAND ----------

# DBTITLE 1,Common Functions
# MAGIC %run /Workspace/Repos/mariobgallardo@gmail.com/bees_breweries_case/databricks/sl_breweries/utils/common

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# DBTITLE 1,Setting parameters
# Setting parameters for further use

API_URL = "https://api.openbrewerydb.org/v1/breweries"
PER_PAGE = 200
TIMEOUT = 30
MAX_RETRIES = 3
TIME_BETWEEN_RETRIES = 2
EXPECTED_MIN = 5000

STORAGE_PATH = "abfss://bronze@stgbeesbreweriescasedev.dfs.core.windows.net/openbrewerydb"

ALLOW_MULTIPLE_RUNS_PER_DAY = False

session = requests.Session()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Usage

# COMMAND ----------

# DBTITLE 1,Main: API extraction
# Creating run parameters
ingestion_date = datetime.utcnow().strftime("%Y-%m-%d")
run_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

day_path = f"{STORAGE_PATH}/ingestion_date={ingestion_date}"
run_path = f"{day_path}/run_id={run_id}"

# Control if multiple runs per day are allowed
day_success = f"{day_path}/_SUCCESS"

if not ALLOW_MULTIPLE_RUNS_PER_DAY and path_exists(day_success):
        raise Exception(f"Ingestion already successfully executed for {ingestion_date}. Aborting execution.")

# Starting bronze ingestion
print(f"[INFO] Starting Bronze ingestion. ingestion_date={ingestion_date}, run_id={run_id}")
print(f"[INFO] Output path: {run_path}")

page = 1
total_records = 0
total_pages = 0

while True:
    raw_bytes = extract_page(page)

    # Stop condition: parse and check list length
    if is_empty_payload(raw_bytes):
        print("[INFO] No more data returned by API. Ending ingestion loop.")
        break

    # parse only for counting
    page_data = parse_payload(raw_bytes)
    total_records += len(page_data)
    total_pages += 1

    output_path = f"{run_path}/breweries_page_{page}.json"
    write_bytes_to_path(raw_bytes, output_path)

    print(f"[INFO] Saved page {page} ({len(page_data)} records) to {output_path}")

    page += 1

print(f"[INFO] Ingestion finished. Total pages={total_pages}, total_records={total_records}")



# COMMAND ----------

# DBTITLE 1,Volume Validation
# Basic volume validation (fail-fast)
if total_records == 0:
    raise Exception("API returned zero records")
elif total_records < EXPECTED_MIN:
    raise Exception(f"Unexpected ingestion volume: {total_records} < {EXPECTED_MIN}")


# COMMAND ----------

# DBTITLE 1,Write SUCCESS Marker
# Write SUCCESS marker ONLY after everything is OK
success_payload = {
    "ingestion_date": ingestion_date,
    "run_id": run_id,
    "total_pages": total_pages,
    "total_records": total_records,
    "per_page": PER_PAGE,
    "api_url": API_URL,
    "finished_at_utc": datetime.utcnow().isoformat()
}
write_success_marker(day_path, success_payload)

# Write Success run pointer
dbutils.fs.put(f"{run_path}/_SUCCESS", json.dumps(success_payload), overwrite=True)

# Write latest run pointer
dbutils.fs.put(f"{day_path}/_LATEST", run_id, overwrite=True)

# Write Success run pointer - only if multiple runs per day are not allowed
if not ALLOW_MULTIPLE_RUNS_PER_DAY:
    dbutils.fs.put(f"{day_path}/_SUCCESS", json.dumps(success_payload), overwrite=True)


print(f"[SUCCESS] Bronze ingestion completed. _SUCCESS written to {run_path}/_SUCCESS")
print(f"[SUCCESS] Latest run pointer updated: {day_path}/_LATEST = {run_id}")
