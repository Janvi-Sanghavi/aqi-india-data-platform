import os
import json
import time
import uuid
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState, StatementParameterListItem

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
SQL_DIR = os.path.join("sql", "pipeline")
COPY_SQL = os.path.join(SQL_DIR, "01_copy_into_bronze.sql")
MERGE_SQL = os.path.join(SQL_DIR, "02_merge_silver.sql")
QUALITY_SQL = os.path.join(SQL_DIR, "03_quality_checks.sql")


# ---------- Helpers ----------
def now_utc():
    return datetime.now(timezone.utc)


def read_sql_file(path):
    """Read a SQL file and remove any trailing semicolon."""
    with open(path, "r", encoding="utf-8") as f:
        return f.read().strip().rstrip(";")


def param(name, value, type_name):
    """Build a named SQL parameter (keeps values separate from the SQL text)."""
    return StatementParameterListItem(
        name=name,
        value=None if value is None else str(value),
        type=type_name,
    )


def run_sql(client, statement, parameters=None):
    """Run one SQL statement on the warehouse and return the result rows as dictionaries."""
    response = client.statement_execution.execute_statement(
        statement=statement,
        warehouse_id=WAREHOUSE_ID,
        wait_timeout="50s",
        parameters=parameters,
    )

    # If still running after 50 seconds, check again every 5 seconds
    while response.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(5)
        response = client.statement_execution.get_statement(response.statement_id)

    if response.status.state != StatementState.SUCCEEDED:
        message = response.status.error.message if response.status.error else "unknown error"
        raise RuntimeError(f"SQL failed: {message}")

    rows = []
    if response.result and response.result.data_array:
        columns = [c.name for c in response.manifest.schema.columns]
        rows = [dict(zip(columns, row)) for row in response.result.data_array]
    return rows


# ---------- Extract ----------
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


def fetch_all_records():
    """Fetch every page from the API."""
    all_records = []
    offset = 0
    total = None

    while total is None or offset < total:
        print(f"Fetching records from offset {offset}...")
        data = fetch_page(offset)
        if data is None:
            raise RuntimeError(f"API page at offset {offset} failed after 3 attempts")

        total = int(data["total"])
        records = data.get("records", [])
        if not records:
            break

        all_records.extend(records)
        offset += PAGE_SIZE

    print(f"Fetched {len(all_records)} of {total} records.")
    return all_records


# ---------- Load ----------
def save_locally(records, fetched_at):
    """Save the raw records as a timestamped JSON file."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    file_name = f"aqi_live_{fetched_at.strftime('%Y%m%d_%H%M%S')}.json"
    file_path = os.path.join(OUTPUT_DIR, file_name)
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump({"fetched_at_utc": fetched_at.isoformat(), "records": records},
                  f, ensure_ascii=False)
    print(f"Saved locally to {file_path}")
    return file_path, file_name


def upload_to_databricks(client, local_path, file_name):
    """Upload the raw JSON file to the bronze volume."""
    client.files.create_directory(VOLUME_DIR)
    target_path = f"{VOLUME_DIR}/{file_name}"
    with open(local_path, "rb") as f:
        client.files.upload(target_path, f, overwrite=True)
    print(f"Uploaded to Databricks: {target_path}")


# ---------- Quality checks and logging ----------
def run_quality_checks(client, run_id):
    """Run all quality checks, print them, and save each result."""
    print("Running quality checks...")
    checks = run_sql(client, read_sql_file(QUALITY_SQL))
    checked_at = now_utc().isoformat()

    for check in checks:
        mark = "PASS" if check["passed"] == "true" else "FAIL"
        print(f"  [{mark}] {check['check_name']}: {check['check_value']} "
              f"(rule {check['threshold']}, {check['severity']})")

        run_sql(
            client,
            """INSERT INTO workspace.aqi.data_quality_results
               VALUES (:run_id, :check_name, :check_value, :threshold,
                       :passed, :severity, :checked_at)""",
            [
                param("run_id", run_id, "STRING"),
                param("check_name", check["check_name"], "STRING"),
                param("check_value", check["check_value"], "DOUBLE"),
                param("threshold", check["threshold"], "STRING"),
                param("passed", check["passed"], "BOOLEAN"),
                param("severity", check["severity"], "STRING"),
                param("checked_at", checked_at, "TIMESTAMP"),
            ],
        )
    return checks


def log_run(client, run):
    """Write one row to pipeline_runs."""
    run_sql(
        client,
        """INSERT INTO workspace.aqi.pipeline_runs
           VALUES (:run_id, :started, :finished, :status, :records_fetched,
                   :files_loaded, :readings_inserted, :checks_failed, :message)""",
        [
            param("run_id", run["run_id"], "STRING"),
            param("started", run["started"], "TIMESTAMP"),
            param("finished", run["finished"], "TIMESTAMP"),
            param("status", run["status"], "STRING"),
            param("records_fetched", run["records_fetched"], "INT"),
            param("files_loaded", run["files_loaded"], "INT"),
            param("readings_inserted", run["readings_inserted"], "INT"),
            param("checks_failed", run["checks_failed"], "INT"),
            param("message", run["message"], "STRING"),
        ],
    )


# ---------- Main ----------
def main():
    run = {
        "run_id": str(uuid.uuid4()),
        "started": now_utc().isoformat(),
        "finished": None,
        "status": "failed",
        "records_fetched": None,
        "files_loaded": None,
        "readings_inserted": None,
        "checks_failed": None,
        "message": None,
    }
    client = None
    print(f"Run ID: {run['run_id']}")

    try:
        if not API_KEY or not WAREHOUSE_ID:
            raise RuntimeError("Missing DATA_GOV_API_KEY or DATABRICKS_WAREHOUSE_ID")

        # 1. Extract
        fetched_at = now_utc()
        records = fetch_all_records()
        run["records_fetched"] = len(records)

        # 2. Load
        file_path, file_name = save_locally(records, fetched_at)
        client = WorkspaceClient()
        upload_to_databricks(client, file_path, file_name)

        # 3. Transform: bronze then silver
        print("Running COPY INTO bronze...")
        copy_result = run_sql(client, read_sql_file(COPY_SQL))
        run["files_loaded"] = int(copy_result[0]["num_inserted_rows"]) if copy_result else 0
        print(f"  Files loaded: {run['files_loaded']}")

        print("Running MERGE into silver...")
        merge_result = run_sql(client, read_sql_file(MERGE_SQL))
        run["readings_inserted"] = int(merge_result[0]["num_inserted_rows"]) if merge_result else 0
        print(f"  Readings inserted: {run['readings_inserted']}")

        # 4. Quality checks
        checks = run_quality_checks(client, run["run_id"])
        failed = [c for c in checks if c["passed"] != "true"]
        critical = [c for c in failed if c["severity"] == "critical"]
        run["checks_failed"] = len(failed)

        if critical:
            run["status"] = "failed"
            run["message"] = "Critical checks failed: " + ", ".join(c["check_name"] for c in critical)
        elif failed:
            run["status"] = "success_with_warnings"
            run["message"] = "Warnings: " + ", ".join(c["check_name"] for c in failed)
        else:
            run["status"] = "success"

    except Exception as error:
        run["status"] = "failed"
        run["message"] = f"{error.__class__.__name__}: {error}"[:500]
        print(f"ERROR: {run['message']}")

    finally:
        # Always try to log the run, even after a failure
        run["finished"] = now_utc().isoformat()
        try:
            if client is None and WAREHOUSE_ID:
                client = WorkspaceClient()
            if client is not None:
                log_run(client, run)
                print("Run logged to pipeline_runs.")
        except Exception as log_error:
            print(f"Could not log run: {log_error.__class__.__name__}")

    print(f"Pipeline finished with status: {run['status']}")
    if run["status"] == "failed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()