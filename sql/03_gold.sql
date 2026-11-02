-- Step 1: Look at the stations data
SELECT * FROM workspace.aqi.bronze_stations LIMIT 10;

-- Step 2: Count stations and cities
SELECT COUNT(*) AS stations,
    COUNT(DISTINCT City) AS cities,
    COUNT(DISTINCT State) AS states
FROM workspace.aqi.bronze_stations;

-- Step 3: Match our 26 cities with their states
SELECT DISTINCT s.city, st.State
FROM workspace.aqi.silver_city_day s
LEFT JOIN workspace.aqi.bronze_stations st
    ON s.city = st.City
ORDER BY s.city;

-- Step 4: Create the city dimension table
CREATE OR REPLACE TABLE workspace.aqi.dim_city AS
SELECT
    ROW_NUMBER() OVER (ORDER BY city) AS city_id,
    city,
    state,
    CASE
        WHEN state IN ('Delhi', 'Haryana', 'Punjab', 'Chandigarh', 'Uttar Pradesh', 'Rajasthan') THEN 'North'
        WHEN state IN ('Gujarat', 'Maharashtra') THEN 'West'
        WHEN state IN ('Andhra Pradesh', 'Karnataka', 'Tamil Nadu', 'Kerala', 'Telangana') THEN 'South'
        WHEN state IN ('Odisha', 'Jharkhand', 'Bihar', 'West Bengal') THEN 'East'
        WHEN state IN ('Madhya Pradesh') THEN 'Central'
        WHEN state IN ('Assam', 'Meghalaya', 'Mizoram') THEN 'Northeast'
    END AS region
FROM (
    SELECT DISTINCT s.city, st.State AS state
    FROM workspace.aqi.silver_city_day s
    LEFT JOIN workspace.aqi.bronze_stations st
        ON s.city = st.City
);

-- Step 5: Check dim_city
SELECT * FROM workspace.aqi.dim_city;

-- Step 6: Create the data dimension table
CREATE OR REPLACE TABLE workspace.aqi.dim_table AS
SELECT
    CAST(DATE_FORMAT(d, 'yyyyMMdd') AS INT) AS date_id,
    d AS date,
    YEAR(d) AS year,
    MONTH(d) AS month,
    DATE_FORMAT(d, 'MMM') AS month_name,
    CASE
        WHEN MONTH(d) IN (12, 1, 2) THEN 'Winter'
        WHEN MONTH(d) IN (3, 4, 5) THEN 'Summer'
        WHEN MONTH(d) IN (6, 7, 8, 9) THEN 'Monsoon'
        ELSE 'Post-Monsoon'
    END AS season,
    DATE_FORMAT(d, 'EEEE') AS day_name,
    DAYOFWEEK(d) IN (1, 7) AS is_weekend
FROM(
    SELECT EXPLODE(SEQUENCE(DATE'2015-01-01', DATE'2027-12-31', INTERVAL 1 DAY)) AS d
);

-- Step 7: Check dim_date
SELECT COUNT(*) AS total_days, MIN(date) AS first_date, MAX(date) AS last_date
FROM workspace.aqi.dim_table;

ALTER TABLE workspace.aqi.dim_table RENAME TO workspace.aqi.dim_date;
SELECT * FROM workspace.aqi.dim_date LIMIT 7;

-- Step 8: Create the pollutant dimension table
CREATE OR REPLACE TABLE workspace.aqi.dim_pollutant AS
SELECT * FROM VALUES
    (1, 'pm25', 'PM2.5', 'µg/m³', 60, '24-hour'),
    (2, 'pm10', 'PM10', 'µg/m³', 100, '24-hour'),
    (3, 'no2', 'Nitrogen dioxide', 'µg/m³', 80, '24-hour'),
    (4, 'so2', 'Sulphur dioxide', 'µg/m³', 80, '24-hour'),
    (5, 'co', 'Carbon monoxide', 'µg/m³', 2, '8-hour'),
    (6, 'o3', 'Ozone', 'µg/m³', 100, '8-hour')
AS t(pollutant_id, pollutant_code, pollutant_name, unit, safe_limit, limit_period);

-- Step 9: Check dim_pollutant
SELECT * FROM workspace.aqi.dim_pollutant;

-- Step 10: Fix CO unit (should be mg/m³)
UPDATE workspace.aqi.dim_pollutant
SET unit = 'mg/m³'
WHERE pollutant_code = 'co';

-- Step 11: Create the fact table
CREATE OR REPLACE TABLE workspace.aqi.fact_aqi_daily AS
SELECT
    c.city_id,
    CAST(DATE_FORMAT(s.date, 'yyyyMMdd') AS INT) AS date_id,
    s.pm25,
    s.pm10,
    s.no2,
    s.so2,
    s.co,
    s.o3,
    s.aqi,
    s.aqi_category,
    s.aqi_more_500,
    'historic' AS source
FROM workspace.aqi.silver_city_day s
JOIN workspace.aqi.dim_city c
    ON s.city = c.city;

-- Step 12: Check the fact table
SELECT COUNT(*) AS total_rows FROM workspace.aqi.fact_aqi_daily;

-- Step 13: Test the star scheme - average AQI by region
SELECT
    c.region,
    COUNT(DISTINCT c.city) AS cities,
    ROUND(AVG(f.aqi), 1) AS avg_aqi
FROM workspace.aqi.fact_aqi_daily f
JOIN workspace.aqi.dim_city c ON f.city_id = c.city_id
JOIN workspace.aqi.dim_date d ON f.date_id = d.date_id
WHERE f.aqi_more_500 = FALSE
GROUP BY c.region
ORDER BY avg_aqi DESC;