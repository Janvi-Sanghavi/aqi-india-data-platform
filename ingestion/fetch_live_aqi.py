import os
import json
import time
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient

# ---------- Settings ----------
load_dotenv()
API_KEY = os.getenv("DATA_GOV_API_KEY")
URL = "https://api.data.gov.in/resource/3b01bcb8-0b14-4abf-b6f2-c1bfd384ba69"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AQI-Project/1.0",
    "Accept": "application/json",
}
PAGE_SIZE = 1000
OUTPUT_DIR = os.path.join("data", "live")
VOLUME_DIR = "/Volumes/workspace/aqi/bronze/live"


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


def upload_to_databricks(local_path, file_name):
    """Upload the raw JSON file to the bronze volume in Databricks."""
    client = WorkspaceClient()  # reads DATABRICKS_HOST and DATABRICKS_TOKEN
    client.files.create_directory(VOLUME_DIR)
    target_path = f"{VOLUME_DIR}/{file_name}"
    with open(local_path, "rb") as f:
        client.files.upload(target_path, f, overwrite=True)
    print(f"Uploaded to Databricks: {target_path}")


def main():
    if not API_KEY:
        print("API key not found. Check your .env file.")
        raise SystemExit(1)

    all_records = []
    offset = 0
    total = None

    # Keep fetching pages until we have every record
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

    # Save the raw records with a timestamp in the file name
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

    # Upload the same file to the Databricks bronze volume
    upload_to_databricks(file_path, file_name)


if __name__ == "__main__":
    main()