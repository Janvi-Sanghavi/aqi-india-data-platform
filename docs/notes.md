# AQI India Data Platform: Project Notes

## Project goal
Help authorities and citizens anticipate poor air quality days and understand
what drives them, using reliable, automatically updated pollution data.

## Data sources
- Historic: Kaggle "Air Quality Data in India (2015-2020)" by Vopani (CPCB data)
  - city_day.csv: daily pollutants and AQI per city
  - stations.csv: station names, cities, states
- Live (upcoming): data.gov.in "Real time Air Quality Index from various locations"

## Platform
Databricks Free Edition (catalog: workspace, schema: aqi)

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

---

## Day 1: Silver layer

### Data quality issues found
1. **Ahmedabad CO sensor error:** CO equals NO in 1,317 of 2,009 rows (66%).
   Two different gases cannot match this often, so this is a recording error.
2. **AQI above 500:** 543 readings exceed India's AQI scale maximum of 500.
   Ahmedabad accounts for 412 of them, likely inflated by the faulty CO values.
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

---

## First insights
- About 26% of recorded days were Poor or worse (6,456 of 24,850)
- Only about 5% of days had Good air quality (1,341 days)
- Severe count is likely inflated by Ahmedabad's faulty CO data

---

## Skills used
- SQL: CREATE SCHEMA/VOLUME/TABLE, read_files, COUNT, DISTINCT, MIN/MAX,
  GROUP BY, ORDER BY, WHERE, CASE WHEN, ALTER TABLE, UPDATE, DESCRIBE HISTORY,
  VERSION AS OF (time travel)
- PySpark: spark.table, filter, select, alias, withColumn, when/otherwise,
  isin, year, month, groupBy, partitionBy, saveAsTable, exceptAll
- Concepts: medallion architecture, Delta tables, data profiling,
  data quality checks, partitioning

---

## Next steps
- Gold layer: star schema (fact and dimension tables)
- Advanced SQL analysis
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
- **dim_date covers 2015-2027**, not just 2015-2020, so live API data
  from 2026 already has matching dates
- **Regions** assigned by state: North, South, East, West, Central, Northeast
- **source column** in fact table ("historic" now, "live" later) to track
  where each row came from
- **Units stored in dim_pollutant**: CO is in mg/m³, others in µg/m³
  (fixed a unit entry error during build)
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
- Next step: compare regions over the same time period for a fair comparison

### Skills used
- SQL: LEFT JOIN, INNER JOIN, table aliases, subqueries, ROW_NUMBER()
  (window function), SEQUENCE + EXPLODE, CAST, DATE_FORMAT, DAYOFWEEK,
  VALUES, UPDATE with WHERE, ALTER TABLE RENAME, ROUND, AVG
- Concepts: star schema, fact vs dimension tables, surrogate keys,
  designing for future data, validating results before trusting them

### Errors fixed
- CASE vs CAST mix-up
- Table created as dim_table instead of dim_date (renamed)
- DATE FORMAT missing underscore (DATE_FORMAT)
- CO unit entered as µg/m³ instead of mg/m³ (fixed with UPDATE)

---

## Next steps
- Advanced SQL analysis: window functions, CTEs, ranking,
  year-over-year comparisons
- Fair comparisons using the same time period for all cities