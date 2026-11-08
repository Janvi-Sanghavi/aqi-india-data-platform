# Runbook: AQI India Data Platform

How to set up, run, and fix the project.

## 1. Local setup

```bash
python -m venv venv
venv\Scripts\activate            # Windows Command Prompt (not PowerShell)
pip install -r requirements.txt
```

Create **`.env`** (pipeline) and **`.streamlit/secrets.toml`** (app). Both are ignored by Git.

`.env`
```
DATA_GOV_API_KEY=...
DATABRICKS_HOST=https://<workspace>.cloud.databricks.com
DATABRICKS_TOKEN=...
DATABRICKS_WAREHOUSE_ID=...
```

`.streamlit/secrets.toml`
```toml
DATABRICKS_HOST = "<workspace>.cloud.databricks.com"
DATABRICKS_HTTP_PATH = "/sql/1.0/warehouses/<warehouse-id>"
DATABRICKS_TOKEN = "..."
GEMINI_API_KEY = "..."
GEMINI_MODEL = "gemini-3.5-flash-lite"
```

## 2. Run the pipeline manually

```bash
python ingestion\fetch_live_aqi.py
```

Expected ending: `Pipeline finished with status: success`.

## 3. Run the app locally

```bash
python -m streamlit run app/app.py
```

Always use `python -m streamlit` so the app runs with the venv's packages.

## 4. Scheduled runs (GitHub Actions)

- Workflow: `.github/workflows/ingest_live_aqi.yml`, every 3 hours (UTC) + manual "Run workflow"
- Runs on the **self-hosted runner** (the government API blocks cloud servers). Start it with:

```powershell
cd C:\actions-runner
./run.cmd
```

Keep the window open; runs are skipped while the laptop is off.

## 5. Monitoring

In Databricks SQL:

```sql
SELECT run_started_utc, status, records_fetched, readings_inserted, checks_failed, message
FROM workspace.aqi.pipeline_runs
ORDER BY run_started_utc DESC;
```

The app's **"Can I trust this data?"** tab shows the same information.

## 6. Common problems

| Symptom | Likely cause | Fix |
|---|---|---|
| Job waits forever on GitHub | Runner not running | Start `./run.cmd` |
| `ConnectTimeout` on GitHub-hosted runner | API blocks cloud IPs | Use the self-hosted runner |
| `502 Bad Gateway` from the API | Missing User-Agent / API down | Header is set; wait and retry |
| Run `failed` with SQL syntax error | More than one statement in a `sql/pipeline/` file | Keep exactly one statement per file |
| `row_count` check fails | Incomplete API response | Check API; next run usually recovers |
| App shows "briefing unavailable" | Gemini error (quota, model renamed) | Check `[briefing error]` in the terminal; update `GEMINI_MODEL` |
| App can't load data | Databricks token expired (90 days) | See section 7 |
| `No module named ...` | Package outside the venv | Activate venv, `pip install -r requirements.txt` |

## 7. Renewing the Databricks token (every 90 days)

Create a new token (Databricks → Settings → Developer → Access tokens) and update it in **four places**:

1. `.env`
2. GitHub → Settings → Secrets → `DATABRICKS_TOKEN`
3. `.streamlit/secrets.toml`
4. Streamlit Cloud → app → Settings → Secrets

## 8. Adding a new city or check

- **New city:** the live pipeline already includes every city the API reports; nothing to change.
- **New quality check:** add a `UNION ALL` block to `sql/pipeline/03_quality_checks.sql` with the same five columns (`check_name`, `check_value`, `threshold`, `passed`, `severity`), and add a plain-language name to `CHECK_NAMES` in `app/app.py`.