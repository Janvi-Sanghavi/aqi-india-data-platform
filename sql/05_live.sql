-- Step 1: Create the bronze live table from all JSON files in the live folder
CREATE OR REPLACE TABLE workspace.aqi.bronze_live_aqi AS
SELECT
    fetched_at_utc,
    r.*,
    _metadata.file_name AS source_file
FROM (
    SELECT fetched_at_utc, EXPLODE(records) AS r, _metadata
    FROM read_files(
        '/Volumes/workspace/aqi/bronze/live/',
        format => 'json'
    )
);

-- Step 2: Check the bronze live table
SELECT COUNT(*) AS total_rows,
    COUNT(DISTINCT source_file) AS files,
    COUNT(DISTINCT city) AS cities,
    COUNT(DISTINCT station) AS stations
FROM workspace.aqi.bronze_live_aqi;

-- Step 3: Pollutants in the live data and missing values
SELECT
    pollutant_id,
    COUNT(*) AS readings,
    SUM(CASE WHEN avg_value = 'NA' THEN 1 ELSE 0 END) AS na_values,
    ROUND(SUM(CASE WHEN avg_value = 'NA' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct_na
FROM workspace.aqi.bronze_live_aqi
GROUP BY pollutant_id
ORDER BY readings DESC;

-- Step 4: Check for duplicate readings (same station, pollutant, and time)
SELECT
    station,
    pollutant_id,
    last_update,
    COUNT(*) AS copies
FROM workspace.aqi.bronze_live_aqi
GROUP BY station, pollutant_id, last_update
HAVING COUNT(*) > 1;

-- Step 5: Inspect the duplicate rows
SELECT station, pollutant_id, last_update, min_value, max_value, avg_value, source_file
FROM workspace.aqi.bronze_live_aqi
WHERE station = 'Sneh Nagar, Nanded - MPCB'
    AND pollutant_id = 'OZONE';

-- Step 6: Create the cleaned silver live table
CREATE OR REPLACE TABLE workspace.aqi.silver_live_aqi AS
SELECT
    state,
    city,
    station,
    TO_TIMESTAMP(last_update, 'dd-MM-yyyy HH:mm:ss') AS reading_time,
    CAST(latitude AS DOUBLE) AS latitude,
    CAST(longitude AS DOUBLE) AS longitude,
    CASE pollutant_id
        WHEN 'PM2.5' THEN 'pm25'
        WHEN 'PM10' THEN 'pm10'
        WHEN 'NO2' THEN 'no2'
        WHEN 'SO2' THEN 'so2'
        WHEN 'CO' THEN 'co'
        WHEN 'OZONE' THEN 'o3'
        WHEN 'NH3' THEN 'nh3'
    END AS pollutant,
    TRY_CAST(NULLIF(min_value, 'NA') AS DOUBLE) AS min_value,
    TRY_CAST(NULLIF(max_value, 'NA') AS DOUBLE) AS max_value,
    TRY_CAST(NULLIF(avg_value, 'NA') AS DOUBLE) AS avg_value,
    CAST(fetched_at_utc AS TIMESTAMP) AS fetched_at_utc, 
    source_file
FROM workspace.aqi.bronze_live_aqi
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY station, pollutant_id, last_update
    ORDER BY fetched_at_utc DESC
) = 1;

-- Step 7: Check the silver live table
SELECT COUNT(*) AS total_rows,
    COUNT(DISTINCT pollutant) AS pollutants,
    SUM(CASE WHEN avg_value IS NULL THEN 1 ELSE 0 END) AS missing_avg,
    MIN(reading_time) AS earliest,
    MAX(reading_time) AS latest
FROM workspace.aqi.silver_live_aqi;