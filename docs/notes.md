
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
- One row per station per pollutant; about 3,900 records per snapshot
- Response has metadata (title, org, field list, updated_date) and records
- Confirmed live: dataset updated on 03/11/2026

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
- Duplicate reading: Sneh Nagar, Nanded (OZONE, 17:00) appeared twice
  with identical values
- Time zone: last_update is IST (Indian local time), but Databricks
  labels it as UTC; values are correct, the label is misleading
- Open question: live values may be on a different basis or unit than
  historic daily averages; check documentation before comparing

### Setup done
- Created virtual environment (venv); used Command Prompt because
  PowerShell blocks scripts by default
- Installed requests, python-dotenv, databricks-sdk
- requirements.txt lists packages for GitHub Actions
- .env holds DATA_GOV_API_KEY, DATABRICKS_HOST, DATABRICKS_TOKEN
  (ignored by Git)
- Databricks personal access token created (90-day lifetime)

### Ingestion script: ingestion/fetch_live_aqi.py
- Reads secrets from environment (.env locally, GitHub Secrets in the cloud);
  the same script works in both places with no changes
- User-Agent header (the server rejected requests that didn't look like
  a browser, returning 502)
- Timeout (90 seconds) so it never waits forever
- Retries: 3 attempts per page with backoff (waits 10s, then 20s)
- Pagination: fetches 1,000 records per page until all are collected
- Safe error messages: prints only error type and status code,
  never the URL (which contains the key)
- Saves raw JSON with a UTC timestamp in the file name
  (e.g. aqi_live_20261103_121837.json), plus fetched_at_utc inside the file
- Uploads the file to /Volumes/workspace/aqi/bronze/live/ with databricks-sdk
- Exits cleanly with SystemExit(1) on failure (marks the run as failed)
- First full run: 4 pages, 3,899 of 3,899 records

### Live tables (sql/05_live.sql)
| Table | Rows (first load) | Description |
|---|---|---|
| bronze_live_aqi | 3,899 | Raw records from all JSON files + source_file (lineage) |
| silver_live_aqi | 3,898 | Cleaned, typed, deduplicated |

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

### Silver live cleaning
- TO_TIMESTAMP for dates, CAST for coordinates
- NULLIF + TRY_CAST: "NA" -> NULL, text -> numbers (TRY_CAST avoids crashing
  on unexpected values)
- Pollutant names mapped to historic names (PM2.5 -> pm25, OZONE -> o3)
- Deduplication with QUALIFY ROW_NUMBER(): keeps the latest fetch per
  station, pollutant, and reading time. Handles exact repeats AND the same
  reading fetched in two runs (DISTINCT alone would miss the second case)

### Automation: GitHub Actions
- Workflow: .github/workflows/ingest_live_aqi.yml
- Schedule: cron "0 */3 * * *" (every 3 hours, UTC); manual trigger
  enabled with workflow_dispatch
- Runs on ubuntu-latest, Python 3.13, 20-minute timeout
- Secrets stored in GitHub Secrets (masked in logs as ***)
- Status: committed; manual test run pending

### Issues encountered
- ReadTimeout: server took more than 30 seconds -> increased timeout,
  added retries
- 502 Bad Gateway: fixed by adding a User-Agent header
- SECURITY: an error message printed the full URL including the API key
  -> changed error handling to never print URLs. The site shows the same
  key and offers no regeneration; risk is low (public data only)
- ModuleNotFoundError: databricks-sdk installed outside venv
  -> reinstalled with (venv) active

### Insight
- India's monitoring network has grown a lot: 26 cities in the 2015-2020
  data vs 289 cities and 557 stations in today's live feed
- Live data is also more complete: PM2.5 missing 8% (vs 16% historically),
  PM10 missing 9% (vs 38% historically)

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
- **SQL:** CREATE SCHEMA/VOLUME/TABLE, read_files (CSV and JSON), COUNT,
  DISTINCT, MIN/MAX, GROUP BY, ORDER BY, WHERE, CASE WHEN, ALTER TABLE
  (ADD COLUMN, RENAME), UPDATE, DESCRIBE HISTORY, VERSION AS OF,
  LEFT/INNER JOIN, aliases, subqueries, CTEs, window functions
  (ROW_NUMBER, RANK, LAG), PARTITION BY, QUALIFY, conditional aggregation,
  HAVING, BETWEEN, IN, SEQUENCE + EXPLODE, CAST, TRY_CAST, NULLIF,
  TO_TIMESTAMP, DATE_FORMAT, DAYOFWEEK, VALUES, ROUND, _metadata
- **PySpark:** spark.table, filter, select, alias, withColumn,
  when/otherwise, isin, year, month, groupBy, partitionBy,
  saveAsTable, exceptAll
- **Python:** requests (params, headers, timeout), python-dotenv,
  os.getenv, os.path, functions, docstrings, dictionaries, while/for loops,
  try/except, f-strings, time.sleep, datetime (UTC), json, file handling,
  databricks-sdk (WorkspaceClient, files.upload), SystemExit,
  if __name__ == "__main__"
- **DevOps:** virtual environments, requirements.txt, .gitignore,
  GitHub Secrets, GitHub Actions (YAML, cron, workflow_dispatch)
- **Concepts:** medallion architecture, Delta tables, time travel,
  data profiling, data quality checks, partitioning, star schema,
  fact vs dimension tables, surrogate keys, fair comparisons,
  validating results before trusting them, retries with backoff,
  pagination, secret management, data lineage, deduplication,
  time zone handling

---

## Next steps
1. Test the GitHub Actions workflow manually (Run workflow)
2. Confirm scheduled runs add new files every 3 hours
3. Incremental load with MERGE (live data into a gold table)
4. Aggregate live station readings to city level
5. Data quality framework + pipeline_runs log
6. Remaining analysis queries + statistics (Day 4)