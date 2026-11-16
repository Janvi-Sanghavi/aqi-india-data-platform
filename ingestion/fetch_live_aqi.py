import base64
import os
import json
import time
import uuid
from datetime import datetime, timezone

import pandas as pd
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
GOLD_SQL = os.path.join(SQL_DIR, "04_gold_live_city_hourly.sql")

# Serving layer: small CSV files published to a separate GitHub branch for the app
GITHUB_REPO = os.getenv("GITHUB_REPOSITORY", "Janvi-Sanghavi/aqi-india-data-platform")
PUBLISH_TOKEN = os.getenv("GH_PUBLISH_TOKEN")
SERVING_BRANCH = "serving-data"
SERVING_DIR = "serving"
SERVING_EXPORTS = {
    "live_latest.csv": """
        SELECT city, state, reading_time, stations_reporting, pm25, pm10
        FROM workspace.aqi.gold_live_city_hourly
        WHERE reading_time = (SELECT MAX(reading_time) FROM workspace.aqi.gold_live_city_hourly)
    """,
    "live_history.csv": """
        SELECT city, reading_time, pm25
        FROM workspace.aqi.gold_live_city_hourly
        WHERE pm25 IS NOT NULL
          AND reading_time >= (SELECT MAX(reading_time) - INTERVAL 7 DAYS
                               FROM workspace.aqi.gold_live_city_hourly)
        ORDER BY city, reading_time
    """,
    "stations.csv": """
        SELECT station, city, state, MAX(latitude) AS lat, MAX(longitude) AS lon
        FROM workspace.aqi.silver_live_aqi
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        GROUP BY station, city, state
    """,
    "forecast_delhi_2019.csv": """
        SELECT date, actual_aqi_tomorrow, baseline_pred, xgb_pred
        FROM workspace.aqi.gold_forecast_delhi_2019
        ORDER BY date
    """,
    "pipeline_runs.csv": """
        SELECT run_id, run_started_utc, status, records_fetched, readings_inserted, message
        FROM workspace.aqi.pipeline_runs
        ORDER BY run_started_utc DESC
        LIMIT 30
    """,
}


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
    if response.result and response.manifest and response.manifest.schema:
        columns = [c.name for c in response.manifest.schema.columns]
        chunk = response.result
        # Large results arrive in several chunks: collect them all
        while chunk is not None:
            rows.extend(dict(zip(columns, row)) for row in (chunk.data_array or []))
            if chunk.next_chunk_index is None:
                break
            chunk = client.statement_execution.get_statement_result_chunk_n(
                response.statement_id, chunk.next_chunk_index
            )
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


# ---------- Serving layer: publish small CSVs to GitHub ----------
def github(method, path, **kwargs):
    """Call the GitHub API with the publish token (never printed)."""
    return requests.request(
        method,
        f"https://api.github.com{path}",
        headers={"Authorization": f"Bearer {PUBLISH_TOKEN}",
                 "Accept": "application/vnd.github+json"},
        timeout=30,
        **kwargs,
    )


def ensure_serving_branch():
    """Create the serving-data branch from main if it doesn't exist yet."""
    if github("GET", f"/repos/{GITHUB_REPO}/branches/{SERVING_BRANCH}").status_code == 200:
        return
    main_ref = github("GET", f"/repos/{GITHUB_REPO}/git/ref/heads/main")
    main_ref.raise_for_status()
    created = github("POST", f"/repos/{GITHUB_REPO}/git/refs",
                     json={"ref": f"refs/heads/{SERVING_BRANCH}",
                           "sha": main_ref.json()["object"]["sha"]})
    created.raise_for_status()
    print(f"Created branch {SERVING_BRANCH}")


def publish_file(name, text):
    """Create or update one file on the serving branch (skips unchanged files)."""
    path = f"/repos/{GITHUB_REPO}/contents/{SERVING_DIR}/{name}"
    existing = github("GET", path, params={"ref": SERVING_BRANCH})
    body = {"message": f"Update {name}", "branch": SERVING_BRANCH,
            "content": base64.b64encode(text.encode("utf-8")).decode("ascii")}
    if existing.status_code == 200:
        current = existing.json()
        if current.get("content"):
            if base64.b64decode(current["content"]).decode("utf-8") == text:
                return "unchanged"
        body["sha"] = current["sha"]
    response = github("PUT", path, json=body)
    response.raise_for_status()
    return "updated"


def publish_serving_data(client, run_id):
    """Export small result tables from Databricks and publish them for the app."""
    if not PUBLISH_TOKEN:
        print("Serving layer: no GH_PUBLISH_TOKEN, skipping publish.")
        return
    print("Publishing serving data...")
    ensure_serving_branch()
    for name, query in SERVING_EXPORTS.items():
        rows = run_sql(client, query)
        result = publish_file(name, pd.DataFrame(rows).to_csv(index=False))
        print(f"  {name}: {len(rows)} rows, {result}")
    checks = run_sql(
        client,
        """SELECT check_name, check_value, threshold, passed, severity
           FROM workspace.aqi.data_quality_results WHERE run_id = :run_id""",
        [param("run_id", run_id, "STRING")],
    )
    result = publish_file("quality_latest.csv", pd.DataFrame(checks).to_csv(index=False))
    print(f"  quality_latest.csv: {len(checks)} rows, {result}")


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

        
        print("Building live gold table...")
        run_sql(client, read_sql_file(GOLD_SQL))
        print("  Gold table refreshed.")

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

        # Publish the serving files (after logging, so the run log includes this run).
        # A publishing problem never changes the pipeline's status.
        try:
            if client is not None:
                publish_serving_data(client, run["run_id"])
        except Exception as publish_error:
            print(f"Could not publish serving data: {publish_error.__class__.__name__}")

    print(f"Pipeline finished with status: {run['status']}")
    if run["status"] == "failed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()