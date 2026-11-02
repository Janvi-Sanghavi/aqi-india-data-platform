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