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
- pandas + SciPy for statistics (Databricks notebook)
- Python 3.13 + VS Code for the live ingestion pipeline
- GitHub Actions (self-hosted runner) for scheduling
- Git + GitHub (repo: aqi-india-data-platform)

## Architecture (medallion)
- **Bronze:** raw data exactly as received (never edited)
- **Silver:** cleaned, corrected, standardized data
- **Gold:** star schema and city-level tables, ready for dashboards and models

## Pipeline flow
```
Historic: Kaggle CSV -> bronze volume -> bronze tables -> silver -> gold

Live (every 3 hours, GitHub Actions on self-hosted runner "janvi-laptop"):
data.gov.in API -> fetch (pagination, retries, User-Agent)
  -> raw JSON saved locally (data/live/)
  -> upload to /Volumes/workspace/aqi/bronze/live/
  -> COPY INTO bronze_live_raw (new files only)
  -> MERGE into silver_live_aqi (new readings only, deduplicated)
  -> rebuild gold_live_city_hourly (city-level, pollutants as columns)
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
   **Update (Day 4):** the other ~131 values above 500 (e.g. Delhi, Nov 2019)
   are REAL extreme pollution days, not errors (see Query 10).
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

## Day 2 and Day 4: Advanced SQL analysis (sql/04_analysis.sql)

### Filter correction (important)
- Queries 1-8 used `aqi_more_500 = false`, which removed ALL values above 500
- Query 10 showed this also removed Delhi's REAL extreme days, understating
  Delhi's peak by about 20%
- From Query 11 onward (and in the statistics notebook), the corrected filter
  excludes only Ahmedabad's flagged values:
  `WHERE NOT (c.city = 'Ahmedabad' AND f.aqi_more_500 = true)`
- Queries 2, 3, 8 therefore slightly understate Delhi's values;
  their conclusions are unchanged

### Query 1: Average AQI per city per year
- Used only the 7 cities with full data since 2015 (fair comparison)

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
- PM2.5 was lower in all 19 cities with data for both years
- Median drop about 38%; most cities fell 20-60%
- Delhi fell 44% (86 to 49 µg/m³)
- Biggest drops: Ahmedabad -63%, Chennai -54%, Gurugram -50%,
  Amritsar -50%, Kolkata -50%
- Industrial towns fell least: Talcher -6%, Brajrajnagar -20%,
  Jorapokhar -22% (hypothesis: some industrial activity continued)
- Mumbai -0.6%: likely a monitoring coverage issue (2019 PM2.5 unusually low)
- Statistically significant (see Day 4 statistics)

### Query 5: Fair city ranking for 2019 (RANK, percentages)
| City | Poor-or-worse days | % |
|---|---|---|
| Delhi | 194 of 365 | 53% |
| Lucknow | 167 of 365 | 46% |
| Patna | 159 of 359 | 44% |
| Gurugram | 154 of 364 | 42% |
- Delhi had Poor or worse air on more than half the days of 2019
- Top cities are all in the Indo-Gangetic plain (North and East India)
- Hyderabad, Bengaluru, Thiruvananthapuram had ZERO Poor days in 2019
- Ahmedabad ranked #1 (95%, avg AQI 516) but excluded as unreliable
- Mumbai (5.5%) looks surprisingly clean, consistent with coverage concerns

### Query 6: Days above the safe PM2.5 limit (2019; CROSS JOIN with dim_pollutant)
| City | Days above 60 µg/m³ | % |
|---|---|---|
| Lucknow | 258 of 365 | 71% |
| Delhi | 243 of 365 | 67% |
| Gurugram | 237 of 364 | 65% |
| Patna | 186 of 359 | 52% |
- Hyderabad had ZERO "Poor" AQI days but exceeded the PM2.5 limit on 23% of
  days: "Moderate" AQI days can already be above the safe limit
- Using PM2.5 instead of AQI corrected Ahmedabad's inflated ranking (95% -> 43%)
- Even the cleanest cities had unsafe days (Bengaluru 7%, Thiruvananthapuram 5%)

### Query 7: Dominant pollutant per city (unpivot + ratio to safe limit, 2019)
- Particulate matter (PM10 or PM2.5) dominated in all 23 cities;
  NO2 and SO2 never dominated
- Delhi PM10 averaged 2.15x the safe limit (highest); Patna 1.97x, Gurugram 1.90x
- Lucknow is dominated by PM2.5 (1.63x), consistent with its 71% unsafe days
- 14 of 23 cities averaged above the limit for their dominant pollutant
- CO and O3 excluded (8-hour limits; CO in different units)
- Caveat: yearly averages vs 24-hour limits; stricter annual limits
  would show even larger exceedances

### Query 8: Weekday vs weekend AQI (2015-2019, 5 reliable cities)
- No meaningful difference: -2.2% to +2.3%, in both directions
- Seasonal and regional factors matter far more than the weekly cycle;
  pollution also carries over between days
- Reporting a null result honestly is part of good analysis

### Query 9: Diwali effect (VALUES, DATE_SUB/DATE_ADD, DATEDIFF; 2015-2019)
- Diwali day + next day vs days -7 to -2

| City | Week before | Diwali | Points | % |
|---|---|---|---|---|
| Chennai | 127.5 | 196.3 | +68.8 | +54% |
| Hyderabad | 105.0 | 143.0 | +38.0 | +36% |
| Lucknow | 301.8 | 368.2 | +66.4 | +22% |
| Delhi | 349.8 | 403.1 | +53.3 | +15% |
- AQI rose in every city; Delhi reaches Severe (403)
- Southern cities show bigger % jumps because they start from cleaner air;
  in absolute points, Chennai and Lucknow rose more than Delhi
- Lesson: percentages and absolute numbers can tell different stories
- Caveats: Diwali falls during the seasonal rise; Bengaluru missing from
  result (to check)

### Query 10: 7-day rolling average (Delhi, Oct-Dec 2019; window frames)
- AQI rose about 4x from 1 Oct (98) to early November
- Peak daily AQI 659 (3 Nov); peak 7-day average 512 (4 Nov)
- Delhi had 7 days above 500 (1, 3, 4, 13, 14, 15 Nov; 30 Dec): REAL extremes
- Sudden clean-ups (89 on 29 Nov) likely from weather (rain/wind)
- **Discovery 1:** the aqi_more_500 filter removed Delhi's real extreme days,
  understating the peak by about 20% (423 vs 512) -> filter corrected
- **Discovery 2:** ROWS BETWEEN 6 PRECEDING counts rows, not days; with
  missing dates the window stretched past 7 days. Switched to
  RANGE BETWEEN INTERVAL 6 DAYS PRECEDING (always 7 calendar days)

### Query 11: Seasonal averages by region (2015-2019, corrected filter)
| Region | Winter | Summer | Monsoon | Post-Monsoon | Winter/Monsoon |
|---|---|---|---|---|---|
| Northeast | 242.6 | 148.0 | 68.6 | 95.0 | 3.54 |
| East | 298.1 | 169.2 | 98.0 | 235.7 | 3.04 |
| Central | 185.5 | null | 73.8 | 169.9 | 2.51 |
| North | 281.9 | 207.1 | 130.4 | 274.5 | 2.16 |
| South | 131.8 | 105.5 | 86.9 | 109.1 | 1.52 |
| West | 238.1 | 220.3 | 218.9 | 221.9 | 1.09 |
- Every reliable region is worst in winter and cleanest in monsoon
  (rain washes particles out; cold, still winter air traps them)
- North: highest absolute levels; even its monsoon (130) is Moderate
- East: sharpest seasonal swing (3.04x)
- South: most stable (1.52x) and cleanest
- Unreliable: West (flat pattern from Ahmedabad's CO error + Mumbai's missing
  years; monsoon 219 unrealistic for Mumbai), Northeast (very little 2019
  data), Central summer NULL (Bhopal starts Sept 2019)

### SQL skills covered in analysis
CTEs, LAG, RANK (with and without PARTITION BY), top-N per group, conditional
aggregation, pivoting and unpivoting, HAVING, CROSS JOIN, window frames
(ROWS vs RANGE), date functions (DATEDIFF, DATE_SUB, DATE_ADD), VALUES,
percentage change, ratios

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
| 26 cities | 289-295 cities, 557 stations |

### Data quality issues in live data
- All values are text, including numbers ("52")
- Missing values written as "NA", not empty
- Date format dd-MM-yyyy HH:mm:ss (historic uses yyyy-MM-dd)
- Ozone named "OZONE" (historic uses O3)
- Includes many more cities than our 26 (e.g. Kadapa, Kakinada)
- Duplicate reading: Sneh Nagar, Nanded (OZONE) appeared twice, identical values
- Stale stations: some stations repeat the same reading across fetches,
  then catch up later (27 readings caught up in one run)
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
| ingestion/fetch_live_aqi.py | Full pipeline: extract, load, transform, gold, checks, logging |
| sql/05_live.sql | Step-by-step live bronze/silver build (development) |
| sql/06_quality.sql | Setup of log tables + monitoring queries |
| sql/07_live_gold.sql | Live gold build + check queries (development) |
| sql/pipeline/01_copy_into_bronze.sql | Incremental bronze load (automated) |
| sql/pipeline/02_merge_silver.sql | Incremental silver MERGE (automated) |
| sql/pipeline/03_quality_checks.sql | Quality checks (automated) |
| sql/pipeline/04_gold_live_city_hourly.sql | Live gold rebuild (automated) |
| .github/workflows/ingest_live_aqi.yml | Schedule: every 3 hours + manual trigger |
| requirements.txt | requests, python-dotenv, databricks-sdk |
- Rule: each file in sql/pipeline/ holds exactly ONE statement
  (the API runs one statement at a time)

### Live tables
| Table | Description |
|---|---|
| bronze_live_raw | One row per file (records stored as a list), loaded with COPY INTO |
| silver_live_aqi | One row per reading: cleaned, typed, deduplicated |
| gold_live_city_hourly | One row per city per hour, pollutants as columns |
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
- Runs within the same hour insert 0 readings (API updates hourly)
- Next hourly update: 3,942 new readings inserted, 27 stale repeats skipped;
  those 27 were inserted in a later run once the stations caught up
- Schema lesson: declaring records as ARRAY<MAP> clashed with the real
  ARRAY<STRUCT>; fixed with a schemaless table + mergeSchema = true

### Live gold layer (gold_live_city_hourly)
- One row per city per hour; pollutants pivoted to columns with
  conditional aggregation (long -> wide, same shape as historic data)
- stations_reporting: number of stations behind each city average
- AVG ignores NULLs, so missing stations don't drag averages down
- Rebuilt fully every run (small and fast; always matches silver).
  Incremental loading is used where data is large (bronze, silver)
- First build: 1,463 rows, 295 cities, 5 hours; Delhi has 59 stations

### Data quality checks (on the latest reading hour in silver)
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
- Typical results: ~3,960 rows, ~8.6% missing, data ~1 hour old
- **Fixed false alarm:** checks originally used the latest FILE in silver,
  which caused a false failure (row_count = 27) after a catch-up run that
  only added 27 readings. Changed to the latest READING HOUR, which always
  contains the full snapshot. Lesson: checks also need testing; false
  alarms make people ignore checks.

### Run logging
- Each run gets a unique run_id (uuid4), linking pipeline_runs and
  data_quality_results
- try / except / finally: the run is ALWAYS logged, even after errors
- Values passed as SQL parameters (prevents SQL injection, e.g. quotes
  in error messages)
- The log captured: failed runs (CREATE TABLE mix-up in the checks file,
  extra SELECT in the gold file, truncated checks file), 1 interrupted
  run (no message, manual stop), 1 false-alarm run, and successful runs
  from both the laptop and GitHub Actions

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
| CREATE / extra SELECT in pipeline files | Whole tab pasted into pipeline file | One statement per pipeline file |
| Syntax error at end of input | Last lines lost while pasting | Check the end of a file after pasting |
| False row_count failure (27) | Check used latest file, not latest hour | Check the latest reading hour |

### Live insights
- Monitoring network grew from 26 cities (2015-2020) to 295 cities, 557 stations
- Live data is more complete: PM2.5 missing ~8.6% (vs 16% historically),
  PM10 ~9% (vs 38%)
- Live data is fresh: newest reading about 1 hour old at fetch time
- Delhi PM2.5 rose from 339 to 369 µg/m³ over 3 days in November 2026
  (about 6x the safe limit of 60), confirming the historic finding that
  November is Delhi's worst month
- Latest PM2.5: Delhi 369, Ahmedabad 131, Mumbai 84, Bengaluru 35
  (Bengaluru below the limit); same North-South pattern as 2015-2020
- Mumbai now has 24 stations (vs very poor historic coverage)
- Note: live times are IST but labeled UTC; compare live values with
  historic averages carefully until the measurement basis is confirmed

---

## Day 4: Statistics (notebook/08_statistics, pandas + SciPy)

### Data
- 24,438 rows (24,850 minus Ahmedabad's 412 flagged rows; corrected filter)
- Loaded with spark.sql(...).toPandas()

### Descriptive statistics
- All pollutants right-skewed (mean > median); a few extreme days pull
  averages up
- PM2.5: median 48 (below the 60 limit) but mean 67 (above it); how you
  summarize changes the message
- Only CO has true zeros (773 days, likely "not measured"); other 0.0
  minimums were rounding (lesson: check exact values before concluding)

### Correlation (Spearman, chosen because data is skewed)
- PM2.5-PM10: 0.88
- AQI most linked to PM10 (0.87) and PM2.5 (0.85); gases 0.27-0.49
- Statistically confirms Query 7: particulates drive AQI
- Ozone weakly linked to everything (0.07 with CO): forms in sunlight
- Caveats: correlation isn't causation (AQI is partly computed from PM2.5);
  pooling cities adds between-city effects
- For forecasting: PM2.5 and PM10 are strong predictors but overlap
  (multicollinearity)

### Outliers (IQR x 3; 1.5 would flag real high-pollution days)
| Pollutant | Upper limit | Outliers | % |
|---|---|---|---|
| SO2 | 41.7 | 1,148 | 4.8% |
| PM2.5 | 232.8 | 718 | 3.0% |
| CO | 3.7 | 574 | 2.5% |
| PM10 | 429.4 | 200 | 1.1% |
| NO2 | 113.9 | 188 | 0.8% |
| O3 | 125.8 | 83 | 0.4% |
| AQI | 553 | 59 | 0.2% |
- PM2.5 outliers are REAL: 83% in Nov-Jan, all Indo-Gangetic cities
  (Patna 222, Delhi 188, Lucknow 147, Gurugram 112)
- Patna has more extreme PM2.5 days than Delhi
- SO2: Jorapokhar and Talcher (coal/industrial) likely real;
  Ahmedabad (407) suspicious, same station as the CO error
  (a station with one known fault is more likely to have others)
- Decision: keep real outliers; they are the events an early-warning
  system must detect

### Lockdown significance test (Wilcoxon signed-rank, paired, one-sided)
- 25 Mar - 31 May, 2019 vs 2020, 19 cities
- Median change -38.2%; statistic 190 (maximum: every city dropped)
- p-value = 0.0000019 (significant, p < 0.001)
- Why Wilcoxon: paired data (each city vs itself) and skewed data
  (no normality assumption, unlike the paired t-test)
- Matches SQL Query 4 (Ahmedabad -57% here vs -63% in SQL due to the
  corrected filter)
- Claim: the drop is real and significant. NOT claimed: that the lockdown
  caused all of it (weather and the pre-existing improvement trend may
  contribute)

---

## Day 5: Forecast model (notebook/09_forecast, pandas + scikit-learn + XGBoost + SHAP)

### Goal
Predict tomorrow's AQI for Delhi, and prove the model beats simple guesses.

### Data preparation
- Delhi only (most complete data: 0.5% AQI missing; biggest need for warnings)
- No AQI filter: Delhi's values above 500 are real (Query 10)
- `asfreq("D")` restores missing dates as empty rows, so "yesterday" always
  means the actual previous day (same lesson as ROWS vs RANGE)
- 2,009 days (2015-01-01 to 2020-07-01); 1,969 rows usable after features

### Target and features
- Target: tomorrow's AQI (`shift(-1)`)
- Features (only today or earlier, to avoid data leakage):
  aqi_today, aqi_lag1, aqi_lag2, aqi_lag7, aqi_roll7 (7-day mean),
  aqi_roll7_std (volatility), pm25_today, pm10_today, month, day_of_year
- Checked shift directions by eye with real rows (target looks forward,
  lags look backward)

### Time-based split (never shuffle time series)
- Train: 2015-01-08 to 2018-12-31 (1,422 rows)
- Test: 2019 (365 rows)
- 2020 excluded from testing (lockdown makes it unusual)
- Random split would leak the future into training

### Results (2019 test year)
| Model | MAE | RMSE |
|---|---|---|
| Baseline: tomorrow = today | 38.6 | 54.1 |
| Baseline: tomorrow = 7-day average | 56.2 | 75.9 |
| **XGBoost** | **32.1** | **44.2** |
- XGBoost: about 17% lower MAE and 18% lower RMSE than the best baseline
- "Tomorrow = today" is a strong baseline (pollution changes slowly)
- 7-day average baseline is worse: it reacts slowly to rising pollution
  (useful as a feature, not as the prediction)
- Bigger RMSE gain suggests the model is especially better on big changes
- XGBoost settings: 300 trees, max_depth 4, learning_rate 0.05,
  subsample 0.8, colsample_bytree 0.8, random_state 42 (reproducible)
- Limitation: no weather data (wind, rain, temperature); adding it would
  likely improve forecasts further

### Early-warning evaluation (AQI > 200 = Poor or worse)
| Model | Poor days | Caught | Missed | False alarms | Recall | Precision |
|---|---|---|---|---|---|---|
| Baseline | 194 | 164 | 30 | 30 | 84.5% | 84.5% |
| **XGBoost** | 194 | **187** | **7** | 39 | **96.4%** | 82.7% |
- Missed warnings dropped 77% (30 -> 7); recall 84.5% -> 96.4%
- Trade-off: 9 more false alarms (precision 84.5% -> 82.7%); favorable for
  public health (a missed dangerous day costs more than an extra warning)
- MAE improved 17%, but missed warnings improved 77%: measure models the way
  they will actually be used

### Explainability (SHAP, mean absolute effect in AQI points)
| Feature | Effect |
|---|---|
| pm25_today | 46.9 |
| aqi_today | 34.3 |
| pm10_today | 15.1 |
| aqi_roll7 | 7.8 |
| aqi_lag2 | 3.7 |
| day_of_year | 3.6 |
| aqi_lag1 | 3.5 |
| aqi_roll7_std | 3.0 |
| aqi_lag7 | 3.0 |
| month | 1.2 |
- Today's PM2.5 is the single most important predictor
- PM2.5 + PM10 explain about half of the model's total effect
- Third independent confirmation that particulates drive AQI:
  SQL (Query 7), statistics (correlation), ML (SHAP)
- Older lags add little: most useful information is in today's readings
- Season features matter less than expected because today's pollution
  already reflects the season

### Output
- Table `workspace.aqi.gold_forecast_delhi_2019` (365 rows): date, city,
  actual_aqi_tomorrow, baseline_pred, xgb_pred (for Power BI and Streamlit)

### Resume line
"Built an XGBoost next-day AQI forecast for Delhi that reduced missed
Poor-air warnings by 77% (30 -> 7) compared with a naive baseline,
catching 96% of unhealthy days in 2019."

---

## Key insights so far (summary)
1. About 26% of recorded days were Poor or worse; only 5% were Good
2. Delhi's average AQI fell 25% from 2015 to 2019, improving every year
3. North India is worst in November (Very Poor); South India stays Moderate
4. In 2019, PM2.5 exceeded the safe limit on 71% of days in Lucknow and
   67% in Delhi; even "zero Poor days" Hyderabad exceeded it on 23% of days
5. Particulate matter dominated in all 23 cities; Delhi PM10 averaged
   2.15x the safe limit
6. PM2.5 dropped in all 19 cities during the 2020 lockdown (median -38%,
   statistically significant, p < 0.001)
7. Delhi's 7-day average peaked at 512 in early November 2019; Diwali
   pushes Delhi into Severe (403)
8. Monitoring coverage grew from 26 cities to 295 cities (557 stations);
   live data (Nov 2026) confirms the November pattern (Delhi PM2.5 ~369)
9. XGBoost next-day forecast for Delhi caught 96% of Poor-or-worse days in
   2019 and cut missed warnings by 77% vs a naive baseline
10. Today's PM2.5 is the top forecast driver (SHAP), confirmed by SQL,
    statistics, and ML
11. Data reliability matters: Ahmedabad's sensor errors, Mumbai's missing
    years, and an over-broad filter would all have produced wrong conclusions
    without checks

---

## Skills used
- **SQL:** CREATE SCHEMA/VOLUME/TABLE (incl. IF NOT EXISTS, schemaless),
  read_files (CSV and JSON), COPY INTO, MERGE, LATERAL VIEW EXPLODE,
  QUALIFY, UNION ALL, COUNT, DISTINCT, MIN/MAX, GROUP BY, ORDER BY, WHERE,
  CASE WHEN, ALTER TABLE (ADD COLUMN, RENAME), UPDATE, DESCRIBE HISTORY,
  VERSION AS OF, LEFT/INNER/CROSS JOIN, aliases, subqueries, CTEs, window
  functions (ROW_NUMBER, RANK, LAG, AVG OVER), window frames (ROWS vs RANGE),
  PARTITION BY, conditional aggregation (pivoting), unpivoting, HAVING,
  BETWEEN, IN, SEQUENCE + EXPLODE, CAST, TRY_CAST, NULLIF, TO_TIMESTAMP,
  FROM_UTC_TIMESTAMP, UNIX_TIMESTAMP, DATE_FORMAT, DATEDIFF, DATE_SUB,
  DATE_ADD, DAYOFWEEK, VALUES, ROUND, _metadata, SHOW TABLES, named parameters
- **PySpark:** spark.table, spark.sql, toPandas, filter, select, alias,
  withColumn, when/otherwise, isin, year, month, groupBy, partitionBy,
  saveAsTable, exceptAll
- **pandas / SciPy:** describe, corr (Spearman), quantile, boolean filtering,
  groupby, pivot_table, apply, lambda, dropna, Wilcoxon signed-rank test,
  to_datetime, set_index, asfreq, shift, rolling
- **Machine learning:** feature engineering (lags, rolling stats, calendar),
  data leakage prevention, time-based split, baselines, XGBoost, MAE/RMSE,
  recall/precision, SHAP explainability, reproducibility (random_state)
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
  data profiling, data quality checks (critical vs warning, false alarms),
  partitioning, star schema, fact vs dimension tables, surrogate keys,
  fair comparisons, validating results before trusting them, retries with
  backoff, pagination, secret management, data lineage, deduplication,
  incremental loading, idempotency, ELT, pivoting (long to wide),
  time zone handling, pipeline monitoring, SQL injection prevention,
  skewed distributions, mean vs median, correlation vs causation,
  outlier detection, paired significance testing, significance vs cause,
  null results

---

## Next steps
1. Power BI dashboard (Day 6)
2. Streamlit app with live AQI, forecast, AI summary, pipeline health (Day 6)
3. Testing + documentation: README, architecture diagram, runbook (Day 7)
4. Optional: rerun Queries 2, 3, 8 with the corrected filter;
   test the forecast on other cities and on 2020