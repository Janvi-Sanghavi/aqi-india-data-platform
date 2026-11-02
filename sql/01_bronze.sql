CREATE SCHEMA IF NOT EXISTS workspace.aqi;
CREATE VOLUME IF NOT EXISTS workspace.aqi.bronze;

CREATE OR REPLACE TABLE workspace.aqi.bronze_city_day AS
SELECT * FROM read_files(
    '/Volumes/workspace/aqi/bronze/city_day.csv',
    format => 'csv',
    header => true
);

CREATE OR REPLACE TABLE workspace.aqi.bronze_stations AS
SELECT * FROM read_files(
    '/Volumes/workspace/aqi/bronze/stations.csv',
    format => 'csv',
    header => true
);

SELECT COUNT(*) AS total_rows, COUNT(DISTINCT City) AS cities, MIN(Date) AS first_date, MAX(Date) AS last_date
FROM workspace.aqi.bronze_city_day;

SELECT * FROM workspace.aqi.bronze_city_day LIMIT 10;

SELECT
  COUNT(*) - COUNT(`PM2.5`) AS pm25_missing,
  COUNT(*) - COUNT(PM10) AS pm10_missing,
  COUNT(*) - COUNT(CO) AS co_missing,
  COUNT(*) - COUNT(SO2) AS so2_missing,
  COUNT(*) - COUNT(O3) AS o3_missing,
  COUNT(*) - COUNT(AQI) AS aqi_missing
FROM workspace.aqi.bronze_city_day;

SELECT City,
    COUNT(*) AS days,
    MIN(Date) AS first_date,
    COUNT(*) - COUNT(AQI) AS aqi_missing
FROM workspace.aqi.bronze_city_day
GROUP BY City
ORDER BY days DESC;