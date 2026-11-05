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
- **Regions** assigned by state: North, South, East,