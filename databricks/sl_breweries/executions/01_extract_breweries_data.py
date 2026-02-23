# Databricks notebook source
# DBTITLE 1,Imports
import requests
import json
import time
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC # Functions

# COMMAND ----------

# DBTITLE 1,Extract Function
"""
    Defining function to extract one page of data from the API
    and save it to the Bronze layer. The function contains the
    retry logic to handle potential errors.

    Args:
        page (int): Page number to extract.
    Returns:
        list: List of dictionaries containing the data for the page.
"""

session = requests.Session()

def extract_page(page: int):
    for attempt in range(1, MAX_RETRIES):
        try:
            response = session.get(
                API_URL,
                params={"page": page,"per_page":PER_PAGE},
                timeout=TIMEOUT,
            )

            response.raise_for_status()

            return response.content

        except Exception as e:
            print(f"[WARNING] Page {page} failed (attempt{attempt}): {e}")

            if attempt == MAX_RETRIES:
                raise

            time.sleep(TIME_BETWEEN_RETRIES)

# COMMAND ----------

# DBTITLE 1,Deduplication control Function
"""
    Check if a path already exists in the storage account.
    
    Args:
        path (str): Path to check.
    Returns:
        bool: True if the path exists, False otherwise.    
"""

def path_exists(path: str) -> bool:
    try:
        dbutils.fs.ls(path)
        return True
    except Exception:
        return False

# COMMAND ----------

# MAGIC %md
# MAGIC # Usage

# COMMAND ----------

# DBTITLE 1,Setting Parameters
# Setting parameters for further use

API_URL = "https://api.openbrewerydb.org/v1/breweries"
PER_PAGE = 200
TIMEOUT = 30
MAX_RETRIES = 3
TIME_BETWEEN_RETRIES = 2

STORAGE_PATH = "abfss://bronze@stgbeesbreweriescasedev.dfs.core.windows.net/openbrewerydb"

ingestion_date = datetime.utcnow().strftime("%Y-%m-%d")
ingestion_tmsp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

today_path = f"{STORAGE_PATH}/ingestion_date={ingestion_date}"

# COMMAND ----------

# DBTITLE 1,Deduplication Control
# Checking if the ingestion for the current day has already been executed

if path_exists(today_path):
    raise Exception(
        f"Ingestion already executed for {ingestion_date}. Aborting execution."
    )

print(f"[INFO] No Ingestion registered for {ingestion_date}. Starting ingestion.")

# COMMAND ----------

# DBTITLE 1,Extract API data
# Loop to extract all pages of data from the API.

page = 1
total_records = 0

while True:

    raw_bytes = extract_page(page)

    # Stops when API return empty data
    if raw_bytes == b"[]":
        print("No more data. Ending ingestion.")
        break

    # Parse data just for counting
    page_data = json.loads(raw_bytes.decode("utf-8"))
    total_records += len(page_data)

    # Setting file destination path
    output_path = (
        f"{STORAGE_PATH}/"
        f"ingestion_date={ingestion_date}/"
        f"breweries_page_{page}_{ingestion_tmsp}.json"
    )

   
    tmp_file = f"/tmp/breweries_page_{page}.json"
    
    with open(tmp_file, "wb") as f:  
        f.write(raw_bytes)
    
    dbutils.fs.cp(f"file:{tmp_file}", output_path)

    print(f"[INFO] Saved page {page} on {output_path}")

    page += 1

print(f"[SUCCESS] Total records ingested: {total_records}")

# COMMAND ----------

# DBTITLE 1,Validation
"""
    This validation step ensures that the ingested data is not empty and meets the expected minimum volume.
"""

EXPECTED_MIN = 5000

if total_records == 0:
    raise Exception("API returned zero records")
elif total_records < EXPECTED_MIN:
    raise Exception("Unexpected ingestion volume")
#else:
#    dbutils.fs.put(success_marker, "SUCCESS")