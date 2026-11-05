import os
import json
import time
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

# ---------- Settings ----------
load_dotenv()
API_KEY = os.getenv("DATA_GOV_API_KEY")
WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID")
URL = "https://api.data.gov.in/resource/3b01bcb8-0b14-4abf-b6f2-c1bfd384ba69"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AQI-Project/1.0",
    "Accept": "application/json",
}
PAGE_SIZE = 1000
OUTPUT_DIR = os.path.join("data", "live")
VOLUME_DIR = "/Volumes/workspace/aqi/bronze/live"
PIPELINE_SQL = [
    os.path.join("sql", "pipeline", "01_copy_into_bronze.sql"),
    os.path.join("sql", "pipeline", "02_merge_silver.sql"),
]


def fetch_page(offset):
    """Fetch one page of records, with up to 3 attempts."""
    params = {
        "api-key": API_KEY,
        "format": "json",
        "limit": PAGE_SIZE,
        "offset": offset,
    }
    for attempt in range(1, 4):
        try:
            response = requests.get(URL, params=params, headers=HEADERS, timeout=90)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as error:
            # Never print the URL (it contains the key)
            status = getattr(error.response, "status_code", "no response")
            print(f"  Offset {offset}, attempt {attempt} failed: "
                  f"{error.__class__.__name__} (status: {status})")
            if attempt < 3:
                time.sleep(10 * attempt)
    return None


def upload_to_databricks(client, local_path, file_name):
    """Upload the raw JSON file to the bronze volume in Databricks."""
    client.files.create_directory(VOLUME_DIR)
    target_path = f"{VOLUME_DIR}/{file_name}"
    with open(local_path, "rb") as f:
        client.files.upload(target_path, f, overwrite=True)
    print(f"Uploaded to Databricks: {target_path}")


def run_sql_file(client, sql_path):
    """Run one SQL file on the Databricks SQL warehouse and wait for it to finish."""
    with open(sql_path, "r", encoding="utf-8") as f:
        statement = f.read().strip().rstrip(";")

    print(f"Running {sql_path}...")
    response = client.statement_execution.execute_statement(
        statement=statement,
        warehouse_id=WAREHOUSE_ID,
        wait_timeout="50s",
    )

    # If the query is still running after 50 seconds, keep checking every 5 seconds
    while response.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(5)
        response = client.statement_execution.get_statement(response.statement_id)

    if response.status.state != StatementState.SUCCEEDED:
        message = response.status.error.message if response.status.error else "unknown error"
        print(f"  FAILED: {message}")
        raise SystemExit(1)

    # Show the result (e.g. num_inserted_rows)
    if response.result and response.result.data_array:
        columns = [c.name for c in response.manifest.schema.columns]
        print(f"  Done: {dict(zip(columns, response.result.data_array[0]))}")
    else:
        print("  Done.")


def main():
    if not API_KEY or not WAREHOUSE_ID:
        print("Missing settings. Check DATA_GOV_API_KEY and DATABRICKS_WAREHOUSE_ID.")
        raise SystemExit(1)

    # ---------- 1. Extract: fetch all pages ----------
    all_records = []
    offset = 0
    total = None

    while total is None or offset < total:
        print(f"Fetching records from offset {offset}...")
        data = fetch_page(offset)
        if data is None:
            print("A page failed after 3 attempts. Stopping.")
            raise SystemExit(1)

        total = int(data["total"])
        records = data.get("records", [])
        if not records:
            break

        all_records.extend(records)
        offset += PAGE_SIZE

    print(f"Fetched {len(all_records)} of {total} records.")

    # ---------- 2. Save the raw file locally ----------
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    fetched_at = datetime.now(timezone.utc)
    file_name = f"aqi_live_{fetched_at.strftime('%Y%m%d_%H%M%S')}.json"
    file_path = os.path.join(OUTPUT_DIR, file_name)

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(
            {"fetched_at_utc": fetched_at.isoformat(), "records": all_records},
            f,
            ensure_ascii=False,
        )
    print(f"Saved locally to {file_path}")

    # ---------- 3. Load: upload to Databricks ----------
    client = WorkspaceClient()  # reads DATABRICKS_HOST and DATABRICKS_TOKEN
    upload_to_databricks(client, file_path, file_name)

    # ---------- 4. Transform: bronze (COPY INTO) and silver (MERGE) ----------
    for sql_path in PIPELINE_SQL:
        run_sql_file(client, sql_path)

    print("Pipeline completed successfully.")


if __name__ == "__main__":
    main()