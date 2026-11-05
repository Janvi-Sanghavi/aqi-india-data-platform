# AQI India Data Platform: Project Notes

## Project goal
Help authorities and citizens anticipate poor air quality days and understand
what drives them, using reliable, automatically updated pollution data.

## Data sources
- **Historic:** Kaggle "Air Quality Data in India (2015-2020)" by Vopani (CPCB data)
  - city_day.csv: daily pollutants and AQI per city
  - stations.csv: station names, cities, states
- **Live:** data.gov.in "Real time Air Quality Index from various locations"
  - Resource ID: 3b01bcb8-0b14-4abf-b6f2-c1bfd384ba69
  - Publisher: CPCB / Ministry of Environment, Forest and Climate Change

## Platform and tools
- Databricks Free Edition (catalog: workspace, schema: aqi)
- SQL and PySpark in Databricks
- Python 3.13 + VS Code for the live ingestion pipeline
- GitHub Actions (self-hosted runner) for scheduling
- Git + GitHub (repo: aqi-india-data-platform)

## Architecture (medallion)
- **Bronze:** raw data exactly as received (never edited)
- **Silver:** cleaned, corrected, standardized data
- **Gold:** star schema, ready for dashboards and models

## Pipeline flow
```
Historic: Kaggle CSV -> bronze volume -> bronze tables -> silver -> gold

Live (every 3 hours, GitHub Actions on self-hosted runner "janvi-laptop"):
data.gov.in API -> fetch (pagination, retries, User-Agent)
  -> raw JSON saved locally (data/live/)
  -> upload to /Volumes/workspace/aqi/bronze/live/
  -> COPY INTO bronze_live_raw (new files only)
  -> MERGE into silver_live_aqi (new readings only, deduplicated)
  -> 6 quality checks -> data_quality_results
  -> run logged -> pipeline_runs (always, even on failure)
```

---

## Day 1: Bronze layer

### What was done
- Created schema `workspace.aqi` and volume `workspace.aqi.bronze`
- Uploaded city_day.csv and stations.csv to the bronze volume
- Created tables `bronze_city_day` and `bronze_stations` using read_files

### Data overview
- 29,531 rows, 26 cities
- Date range: 2015-01-01 to 2020-07-01

### Missing values (out of 29,531 rows)
| Column | Missing | % |
|---|---|---|
| PM10 | 11,140 | 38% |
| AQI | 4,681 | 16% |
| PM2.5 | 4,598 | 16% |
| O3 | 4,022 | 14% |
| SO2 | 3,854 | 13% |
| CO | 2,059 | 7% |

### City coverage
- Only 7 cities have full data from Jan 2015: Delhi, Mumbai, Bengaluru,
  Chennai, Lucknow, Ahmedabad, Hyderabad (from 4 Jan 2015)
- 8 cities started in 2019 or 2020; Aizawl has only 113 days
- Mumbai is missing 61% of AQI values; Ahmedabad 34%; Delhi only 0.5%
- Mumbai has NO AQI data at all for 2015-2017 (whole years empty,
  not random gaps)

---

## Day 1: Silver layer

### Data quality issues found
1. **Ahmedabad CO sensor error:** CO equals NO in 1,317 of 2,009 rows (66%).
   Two different gases cannot match this often, so this is a recording error.
2. **AQI above 500:** 543 readings exceed India's AQI scale maximum of 500.
   Ahmedabad accounts for 412 of them, likely inflated by the faulty CO values.
   Example: 2 Feb 2015, AQI 914 with PM2.5 only 178 (PM2.5 alone would give
   roughly 300-400), so AQI was pushed up by the faulty CO.
3. **Zeros in Benzene and Toluene:** may mean "not measured" rather than zero.

### Cleaning decisions
- Removed rows without AQI (24,850 rows remain)
- Renamed columns to lowercase (PM2.5 -> pm25)
- Set Ahmedabad CO to NULL where CO = NO (kept the row, removed only the bad value)
- Added flag `aqi_more_500` instead of deleting suspicious rows
- Added year, month, season (Winter, Summer, Monsoon, Post-Monsoon)
- Renamed AQI_Bucket to aqi_category

### Silver tables
- `silver_city_day`: built in SQL
- `silver_city_day_spark`: built in PySpark, partitioned by year
- Verified identical: both 24,850 rows, 0 rows differ (checked with exceptAll)

### Season row counts
| Season | Days |
|---|---|
| Monsoon | 7,713 |
| Summer | 7,069 |
| Winter | 6,243 |
| Post-Monsoon | 3,825 |
Note: seasons have different lengths, so compare averages, not counts.

### AQI category counts
| Category | Days | % |
|---|---|---|
| Moderate | 8,829 | 36% |
| Satisfactory | 8,224 | 33% |
| Poor | 2,781 | 11% |
| Very Poor | 2,337 | 9% |
| Good | 1,341 | 5% |
| Severe | 1,338 | 5% |

### Delta features used
- DESCRIBE HISTORY: every change to the table is recorded (25 versions)
- Time travel: `VERSION AS OF 0` showed the first silver table
  (city, date, aqi only)
- Lesson: use "Run selected", not "Run all" (Run all created extra versions)

---

## Day 2: Gold layer (star schema)

### Stations data
- bronze_stations: 230 stations, 127 cities, 21 states
- One city can have several stations; the table includes cities not in city_day
- Matched our 26 cities to their states with a LEFT JOIN: all 26 matched,
  no nulls, no duplicates

### Gold tables
| Table | Rows | Description |
|---|---|---|
| dim_city | 26 | city_id, city, state, region |
| dim_date | 4,748 | date_id, date, year, month, month_name, season, day_name, is_weekend |
| dim_pollutant | 6 | pollutant_id, code, name, unit, safe_limit, limit_period |
| fact_aqi_daily | 24,850 | city_id, date_id, pollutants, aqi, aqi_category, aqi_more_500, source |

### Design decisions
- **IDs instead of names** in the fact table: smaller storage, faster joins;
  names stored once in dimension tables
- **city_id** created with ROW_NUMBER() (alphabetical order)
- **date_id** in yyyyMMdd format (e.g. 20150129): unique and readable
- **dim_date covers 2015-2027** (4,748 days incl. 3 leap days), so live
  API data from 2026 already has matching dates
- **Regions** assigned by state: North, South, East, West, Central, Northeast
- **source column** in fact table ("historic" now, "live" later) to track
  where each row came from
- **Units stored in dim_pollutant**: CO is in mg/m³, others in µg/m³
- Safe limits from India's National Ambient Air Quality Standards (NAAQS)

### Region distribution
South 9, North 6, East 5, Northeast 3, West 2, Central 1

### Star schema check: average AQI by region (excluding AQI > 500)
| Region | Cities | Avg AQI |
|---|---|---|
| West | 2 | 206.6 |
| North | 6 | 195.1 |
| East | 5 | 180.9 |
| Central | 1 | 132.8 |
| Northeast | 3 | 101.9 |
| South | 9 | 101.4 |

### Insight (with caveats)
- North and East India show consistently high average AQI (195 and 181)
- West appears highest (207), but this is NOT reliable:
  - only 2 cities (Ahmedabad, Mumbai)
  - Ahmedabad's faulty CO values inflate AQI even below 500
  - Mumbai is missing 61% of AQI values
- Northeast and Central cities only have 2019-2020 data, including the 2020
  lockdown, so their averages may look better partly due to timing
- South India has the cleanest air (101) across 9 cities, the most reliable
  regional result

### Errors fixed
- CASE vs CAST mix-up
- Table created as dim_table instead of dim_date (renamed)
- DATE FORMAT missing underscore (DATE_FORMAT)
- CO unit entered as µg/m³ instead of mg/m³ (fixed with UPDATE)

---

## Day 2: Advanced SQL analysis

### Query 1: Average AQI per city per year
- Used only the 7 cities with full data since 2015 (fair comparison)
- Excluded AQI > 500 readings

### Query 2: Year-over-year change (CTE + LAG)
- Used 2015-2019 only and 5 reliable cities
  (excluded Mumbai: no early data; Ahmedabad: sensor error)
- 2020 excluded from trends: data ends in July (no Oct-Dec winter months)
  and includes the COVID lockdown

| City | 2015 | 2019 | Change |
|---|---|---|---|
| Delhi | 297.0 | 223.1 | -25% |
| Chennai | 148.3 | 102.9 | -31% |
| Hyderabad | 140.2 | 94.0 | -33% |
| Bengaluru | 112.6 | 91.6 | -19% |
| Lucknow | 191.2 | 202.6 | +6% |

**Insights**
- Delhi and Hyderabad improved every single year from 2015 to 2019
- Largest single-year drops in 2017 (Delhi -35.6, Chennai -34.1)
  - Open question: what changed in 2017? (not yet researched)
- Lucknow jumped +48 in 2016 and shows no net improvement over the period
- Bengaluru worsened slightly in 2019 (+5.3) after three years of improvement

### Query 3: Worst month per city (two CTEs + RANK, top-N per group)
| City | Worst month | Avg AQI | Category |
|---|---|---|---|
| Delhi | Nov | 370 | Very Poor |
| Lucknow | Nov | 363 | Very Poor |
| Hyderabad | Jan | 140 | Moderate |
| Chennai | Jan | 128 | Moderate |
| Bengaluru | Jan | 112 | Moderate |

**Insight:** North Indian cities peak in November (about 365, Very Poor);
South Indian cities peak in January but stay Moderate.
November is commonly associated with crop residue burning, Diwali, and
still winter air (not proven by this data).
**Recommendation:** alerts and interventions in North India should focus
on October-November.

### Query 4: Lockdown effect (conditional aggregation, HAVING)
- Compared PM2.5 for the same dates (25 Mar - 31 May) in 2019 vs 2020
- Used PM2.5 instead of AQI: a direct measurement, avoids the Ahmedabad CO issue

**Insights**
- PM2.5 was lower in all 19 cities with data for both years
- Median drop about 38%; most cities fell 20-60%
- Delhi fell 44% (86 to 49 µg/m³)
- Biggest drops: Ahmedabad -63%, Chennai -54%, Gurugram -50%,
  Amritsar -50%, Kolkata -50%
- Industrial towns fell least: Talcher -6%, Brajrajnagar -20%,
  Jorapokhar -22% (hypothesis: some industrial activity continued)
- Mumbai -0.6%: likely a monitoring coverage issue (2019 PM2.5 unusually low)
- Caveat: weather differs between years, so not all of the drop is
  caused by the lockdown. Statistical test planned.

### Query 5: Fair city ranking for 2019 (RANK, percentages)
- Share of Poor-or-worse days in 2019, cities with 300+ days of data

| City | Poor-or-worse days | % |
|---|---|---|
| Delhi | 194 of 365 | 53% |
| Lucknow | 167 of 365 | 46% |
| Patna | 159 of 359 | 44% |
| Gurugram | 154 of 364 | 42% |

**Insights**
- Delhi had Poor or worse air on more than half the days of 2019
- Top cities are all in the Indo-Gangetic plain (North and East India)
- Hyderabad, Bengaluru, Thiruvananthapuram had ZERO Poor days in 2019
- Ahmedabad ranked #1 (95%, avg AQI 516) but excluded as unreliable
  (CO sensor error; this query did not filter AQI > 500)
- Mumbai (5.5%) looks surprisingly clean, consistent with coverage concerns

### Remaining analysis queries (planned for Day 4)
- Dominant pollutant per city
- Weekday vs weekend AQI
- Diwali effect
- 7-day rolling average
- Seasonal averages by region
- Days exceeding safe limits (using dim_pollutant)

---

## Day 3: Live API ingestion

### API explored
- One row per station per pollutant; about 3,900-3,970 records per snapshot
- Response has metadata (title, org, field list, updated_date) and records
- API updates hourly

### Live data vs historic data
| Historic (Kaggle) | Live API |
|---|---|
| Wide: one row per city per day | Long: one row per station per pollutant |
| Pollutants as columns | pollutant_id as rows (PM10, CO, OZONE...) |
| City-level daily averages | Station-level hourly readings |
| 26 cities | 289 cities, 557 stations |

### Data quality issues in live data
- All values are text, including numbers ("52")
- Missing values written as "NA", not empty
- Date format dd-MM-yyyy HH:mm:ss (historic uses yyyy-MM-dd)
- Ozone named "OZONE" (historic uses O3)
- Includes many more cities than our 26 (e.g. Kadapa, Kakinada)
- Duplicate reading: Sneh Nagar, Nanded (OZONE) appeared twice, identical values
- Stale stations: some stations repeat the same reading across fetches
- Time zone: last_update is IST, but Databricks labels it UTC
- Open question: live values may be on a different basis or unit than
  historic daily averages; check documentation before comparing

### Live data profile (first snapshot, 03-11-2026 17:00 IST)
| Pollutant | Readings | % NA |
|---|---|---|
| OZONE | 558 | 5.7% |
| NO2 | 557 | 7.2% |
| CO | 557 | 4.1% |
| NH3 | 557 | 17.1% |
| PM10 | 557 | 8.6% |
| SO2 | 557 | 11.1% |
| PM2.5 | 556 | 8.3% |
- OZONE 558 (one duplicate); PM2.5 556 (one station did not report)

### Files
| File | Purpose |
|---|---|
| ingestion/fetch_live_aqi.py | Full pipeline: extract, load, transform, checks, logging |
| sql/05_live.sql | Step-by-step live bronze/silver build (development) |
| sql/06_quality.sql | Setup of log tables + monitoring queries |
| sql/pipeline/01_copy_into_bronze.sql | Incremental bronze load (automated) |
| sql/pipeline/02_merge_silver.sql | Incremental silver MERGE (automated) |
| sql/pipeline/03_quality_checks.sql | Quality checks (automated) |
| .github/workflows/ingest_live_aqi.yml | Schedule: every 3 hours + manual trigger |
| requirements.txt | requests, python-dotenv, databricks-sdk |

### Live tables
| Table | Description |
|---|---|
| bronze_live_raw | One row per file (records stored as a list), loaded with COPY INTO |
| silver_live_aqi | One row per reading: cleaned, typed, deduplicated |
| pipeline_runs | One row per pipeline run (logbook) |
| data_quality_results | One row per check per run |
| bronze_live_aqi | Earlier development version (rebuilt from all files) |

### Ingestion script features
- Secrets read from environment (.env locally, GitHub Secrets in the cloud);
  the same script works in both places with no changes
- User-Agent header (server rejected requests that didn't look like a browser)
- 90-second timeout; 3 retries per page with backoff (10s, then 20s)
- Pagination: 1,000 records per page until all are collected
- Safe error messages: error type and status code only, never the URL
- Raw JSON saved with a UTC timestamp in the file name, plus fetched_at_utc
- Upload to the bronze volume with databricks-sdk
- SQL run on the SQL warehouse through the Statement Execution API
- Code organized into sections: helpers, extract, load, quality, main

### Silver cleaning
- TO_TIMESTAMP for dates, CAST for coordinates
- NULLIF + TRY_CAST: "NA" -> NULL, text -> numbers (TRY_CAST avoids crashing)
- Pollutant names mapped to historic names (PM2.5 -> pm25, OZONE -> o3)
- Deduplication: QUALIFY ROW_NUMBER() keeps the latest fetch per
  station + pollutant + reading time (DISTINCT alone would miss
  the same reading fetched in two runs)

### Incremental loading
- **COPY INTO** remembers which files were loaded; only new files are added
  (tested: 2nd run inserted 0 rows)
- **MERGE** inserts only readings that don't already exist
  (match on station + pollutant + reading_time; WHEN NOT MATCHED THEN INSERT)
- Both are **idempotent**: rerunning gives the same result
- COPY INTO catches files missed while away (loaded 2 files at once after a gap)
- First automated run: 7,871 readings inserted from 2 files
- A run 36 minutes after the previous one inserted 0 readings
  (API updates hourly; all readings already loaded)
- Next hourly update: 3,942 new readings inserted, 27 stale repeats skipped
- Schema lesson: declaring records as ARRAY<MAP> clashed with the real
  ARRAY<STRUCT>; fixed with a schemaless table + mergeSchema = true

### Data quality checks (on the latest file in silver)
| Check | Rule | Severity | Why |
|---|---|---|---|
| row_count | >= 1000 | critical | Detects incomplete API responses |
| missing_avg_pct | <= 25% | warning | Detects many stations down |
| negative_values | = 0 | warning | Pollution can't be negative |
| unknown_pollutant | = 0 | warning | Detects renamed/new pollutants |
| duplicate_readings | = 0 | critical | Proves deduplication works (all of silver) |
| data_age_hours | <= 3 | warning | Freshness (IST/UTC converted correctly) |
- Critical failure -> run marked failed (exit code 1, red run on GitHub)
- Warning failure -> success_with_warnings
- First results: all 6 passed (3,962 rows, 8.73% missing, data 0.83 hours old)
- Known limitation: checks evaluate the latest file that has readings in silver

### Run logging
- Each run gets a unique run_id (uuid4), linking pipeline_runs and
  data_quality_results
- try / except / finally: the run is ALWAYS logged, even after errors
- Values passed as SQL parameters (prevents SQL injection, e.g. quotes
  in error messages)
- The log already captured: 2 failed runs (CREATE TABLE mix-up in the
  checks file), 1 interrupted run (no message, manual stop), and
  successful runs from both the laptop and GitHub Actions

### Automation: GitHub Actions
- GitHub cloud runners could NOT connect to the API (ConnectTimeout);
  the government API blocks cloud IPs
- Solution: self-hosted runner on the laptop (C:\actions-runner, name
  "janvi-laptop"); GitHub still schedules, logs, and stores secrets
- Trade-off: runs only happen while the laptop is on and the runner is running;
  missed runs are skipped, the next run catches up
- shell: cmd (avoids PowerShell script restrictions); fresh venv each run
- Per-step timeouts so a stuck step fails fast at the exact step
- PYTHONUNBUFFERED=1 so logs appear live
- Secrets: DATA_GOV_API_KEY, DATABRICKS_HOST, DATABRICKS_TOKEN,
  DATABRICKS_WAREHOUSE_ID (GitHub Secrets, masked in logs)
- Schedule: cron "0 */3 * * *" (every 3 hours, UTC) + workflow_dispatch
- "Abandoned" run: laptop slept during a job; harmless (idempotent pipeline)
- First full automated run on GitHub: success in 1m 19s

### Issues encountered and fixes
| Issue | Cause | Fix |
|---|---|---|
| ReadTimeout | Slow API | 90s timeout + retries with backoff |
| 502 Bad Gateway | Server rejected non-browser requests | User-Agent header |
| API key in error output | Error printed full URL | Never print URLs; status code only |
| PowerShell blocked venv | Execution policy | Used Command Prompt |
| ModuleNotFoundError | Package installed outside venv | Reinstalled with (venv) active |
| requirements.txt missing on GitHub | *.txt in .gitignore | Added !requirements.txt |
| ConnectTimeout on GitHub | API blocks cloud IPs | Self-hosted runner |
| Job waiting 24 min | Runner not started | Start run.cmd before triggering |
| COPY INTO type error | Hand-declared schema didn't match | Schemaless table + mergeSchema |
| CREATE in checks file | Whole tab pasted into pipeline file | One statement per pipeline file |

### Insights
- Monitoring network grew from 26 cities (2015-2020) to 289 cities, 557 stations
- Live data is more complete: PM2.5 missing ~8.7% (vs 16% historically),
  PM10 ~9% (vs 38%)
- Live data is fresh: newest reading under 1 hour old at fetch time

---

## Key insights so far (summary)
1. About 26% of recorded days were Poor or worse; only 5% were Good
2. Delhi's average AQI fell 25% from 2015 to 2019, improving every year
3. North India is worst in November (Very Poor); South India stays Moderate
4. PM2.5 dropped in all 19 cities during the 2020 lockdown (median -38%)
5. Delhi had Poor or worse air on 53% of days in 2019
6. Monitoring coverage grew from 26 cities to 289 cities (557 stations)
7. Data reliability matters: Ahmedabad's CO sensor error and Mumbai's
   missing years would have produced wrong conclusions without checks

---

## Skills used
- **SQL:** CREATE SCHEMA/VOLUME/TABLE (incl. IF NOT EXISTS, schemaless),
  read_files (CSV and JSON), COPY INTO, MERGE, LATERAL VIEW EXPLODE,
  QUALIFY, UNION ALL, COUNT, DISTINCT, MIN/MAX, GROUP BY, ORDER BY, WHERE,
  CASE WHEN, ALTER TABLE (ADD COLUMN, RENAME), UPDATE, DESCRIBE HISTORY,
  VERSION AS OF, LEFT/INNER JOIN, aliases, subqueries, CTEs, window functions
  (ROW_NUMBER, RANK, LAG), PARTITION BY, conditional aggregation, HAVING,
  BETWEEN, IN, SEQUENCE + EXPLODE, CAST, TRY_CAST, NULLIF, TO_TIMESTAMP,
  FROM_UTC_TIMESTAMP, UNIX_TIMESTAMP, DATE_FORMAT, DAYOFWEEK, VALUES,
  ROUND, _metadata, SHOW TABLES, named parameters
- **PySpark:** spark.table, filter, select, alias, withColumn,
  when/otherwise, isin, year, month, groupBy, partitionBy,
  saveAsTable, exceptAll
- **Python:** requests (params, headers, timeout), python-dotenv,
  os.getenv, os.path, functions, docstrings, dictionaries, lists,
  while/for loops, try/except/finally, f-strings, time.sleep,
  datetime (UTC), uuid, json, file handling, databricks-sdk
  (WorkspaceClient, files.upload, statement_execution), SystemExit,
  if __name__ == "__main__"
- **DevOps:** virtual environments, requirements.txt, .gitignore
  (incl. ! exceptions), GitHub Secrets, GitHub Actions (YAML, cron,
  workflow_dispatch, self-hosted runner, per-step timeouts)
- **Concepts:** medallion architecture, Delta tables, time travel,
  data profiling, data quality checks (critical vs warning), partitioning,
  star schema, fact vs dimension tables, surrogate keys, fair comparisons,
  validating results before trusting them, retries with backoff,
  pagination, secret management, data lineage, deduplication,
  incremental loading, idempotency, ELT, time zone handling,
  pipeline monitoring, SQL injection prevention

---

## Next steps
1. Live gold layer: aggregate station readings to city level
2. Remaining analysis queries + statistics (Day 4)
3. Forecast model + SHAP (Day 5)
4. Power BI + Streamlit (Day 6)
5. Testing + documentation (Day 7)