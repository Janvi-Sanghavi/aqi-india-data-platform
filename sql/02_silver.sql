-- NOTE: This file shows how the silver table was built step by step.
-- Each rebuild adds new columns. Step 16 is the final silver build.
-- Use "Run selected" only. Do not use "Run all".
-- Step 1: Create the basic silver table
-- Reads from bronze, renames columns to lowercase,
-- and keeps only rows that have an AQI value
CREATE OR REPLACE TABLE workspace.aqi.silver_city_day AS
SELECT City AS city, Date as date, AQI as aqi
FROM workspace.aqi.bronze_city_day
WHERE AQI IS NOT NULL;

-- Step 2: Check the first 10 rows of the silver table
SELECT * FROM workspace.aqi.silver_city_day LIMIT 10;

-- Step 3: Data quality check
-- India's AQI scale ends at 500, so values above 500 are suspicious
-- Count such days for each city
SELECT city, COUNT(*) AS days_above_500
FROM workspace.aqi.silver_city_day
WHERE aqi > 500
GROUP BY city
ORDER BY days_above_500 DESC;

-- Step 4: Add a flag column for AQI above 500
-- BOOLEAN = true/false
ALTER TABLE workspace.aqi.silver_city_day
ADD COLUMN aqi_more_500 BOOLEAN;

-- Step 5: Fill the flag
-- true if AQI is above 500, otherwise false
UPDATE workspace.aqi.silver_city_day
SET aqi_more_500 = (aqi > 500);

-- Step 6: Check the flag
-- Expected: true = 543, false = 24,307
SELECT aqi_more_500, COUNT(*) AS days
FROM workspace.aqi.silver_city_day
GROUP BY aqi_more_500;

-- Step 7: Rebuild silver with pollutant columns and the AQI flag
CREATE OR REPLACE TABLE workspace.aqi.silver_city_day AS
SELECT
    City AS city,
    Date AS date,
    `PM2.5` AS pm25,
    PM10 AS pm10,
    NO2 AS no2,
    SO2 AS so2,
    O3 AS o3,
    AQI AS aqi,
    AQI > 500 AS aqi_more_500
FROM workspace.aqi.bronze_city_day
WHERE AQI IS NOT NULL;

-- Step 8: Check the new columns
SELECT * FROM workspace.aqi.silver_city_day LIMIT 10;

-- Step 9: How many Ahmedabad rows have CO equal to NO?
SELECT
    COUNT(*) AS total_rows,
    SUM(CASE WHEN CO = NO THEN 1 ELSE 0 END) AS co_equals_no
FROM workspace.aqi.bronze_city_day
WHERE City = 'Ahmedabad';

-- Step 10: Rebuild silver with CO (Ahmedabd CO = NO error set to NULL)
CREATE OR REPLACE TABLE workspace.aqi.silver_city_day AS
SELECT
    City AS city,
    Date AS date,
    `PM2.5` AS pm25,
    PM10 AS pm10,
    NO2 AS no2,
    SO2 AS so2,
    O3 AS o3,
    CASE
        WHEN City = 'Ahmedabad' AND CO = NO THEN NUll
        ELSE CO
    END AS co,
    AQI AS aqi,
    AQI > 500 AS aqi_more_500
FROM workspace.aqi.bronze_city_day
WHERE AQI IS NOT NULL;

-- Step 11: Check the CO fix for Ahmedabad
SELECT city, date, co, aqi
FROM workspace.aqi.silver_city_day
WHERE city = 'Ahmedabad'
LIMIT 10;

-- Step 12: Rebuild silver with year and month
CREATE OR REPLACE TABLE workspace.aqi.silver_city_day AS
SELECT
    City AS city,
    Date AS date,
    YEAR(Date) AS year,
    MONTH(date) AS month,
    `PM2.5` AS pm25,
    PM10 AS pm10,
    NO2 AS no2,
    SO2 AS so2,
    O3 AS o3,
    CASE
        WHEN City = 'Ahmedabd' AND CO = NO THEN NULL
        ELSE CO
    END AS co,
    AQI AS aqi,
    AQI > 500 AS aqi_more_500
FROM workspace.aqi.bronze_city_day
WHERE AQI IS NOT NULL;

-- Step 13: Check year and month
SELECT city, date, year, month, aqi
FROM workspace.aqi.silver_city_day
LIMIT 10;

--Step 14: Rebuild silver with season
CREATE OR REPLACE TABLE workspace.aqi.silver_city_day AS
SELECT
    City AS city,
    Date AS date, 
    YEAR(Date) AS year,
    MONTH(Date) AS month,
    CASE
        WHEN MONTH(Date) IN (12, 1, 2) THEN 'Winter'
        WHEN MONTH(Date) IN (3, 4, 5) THEN 'Summer'
        WHEN MONTH(Date) IN (6, 7, 8, 9) THEN 'Monsoon'
        ELSE 'Post-Monsoon'
    END AS season,
    `PM2.5` AS pm25,
    PM10 AS pm10,
    NO2 AS no2,
    SO2 AS so2,
    O3 AS o3,
    CASE
        WHEN City = 'Ahmedabad' AND CO = NO THEN NULL 
        ELSE CO 
    END AS co,
    AQI AS aqi,
    AQI > 500 AS aqi_more_500
FROM workspace.aqi.bronze_city_day
WHERE AQI IS NOT NULL;

-- Step 15: Check how many days fall in each season
SELECT season, COUNT(*) AS days
FROM workspace.aqi.silver_city_day
GROUP BY season;

-- Step 16 (FINAL): Rebuild silver with AQI category
CREATE OR REPLACE TABLE workspace.aqi.silver_city_day AS
SELECT
    City AS city,
    Date AS date,
    YEAR(Date) AS year,
    MONTH(Date) AS month,
    CASE
        WHEN MONTH(Date) IN (12, 1, 2) THEN 'Winter'
        WHEN MONTH(Date) IN (3, 4, 5) THEN 'Summer'
        WHEN MONTH(Date) IN (6, 7, 8, 9) THEN 'Monsoon'
        ELSE 'Post-Monsoon'
    END AS season,
    `PM2.5` AS pm25,
    PM10 AS pm10,
    NO2 AS no2,
    SO2 AS so2,
    O3 AS o3,
    CASE
        WHEN City = 'Ahmedabad' AND CO = NO THEN NULL
        ELSE CO
    END AS co,
    AQI AS aqi,
    AQI_Bucket AS aqi_category,
    AQI > 500 AS aqi_more_500
FROM workspace.aqi.bronze_city_day
WHERE AQI IS NOT NULL;

-- Step 17: Count days in each AQI Category
SELECT aqi_category, COUNT(*) AS days
FROM workspace.aqi.silver_city_day
GROUP BY aqi_category
ORDER BY days DESC;

-- Step 18: See every change made to the silver table
DESCRIBE HISTORY workspace.aqi.silver_city_day;

-- Step 19: Look at silver as it was in version 0 (the first version)
SELECT * FROM workspace.aqi.silver_city_day VERSION AS OF 0 LIMIT 5;