-- Live gold: One row per city per hour, pollutants as columns
CREATE OR REPLACE TABLE workspace.aqi.gold_live_city_hourly AS
SELECT
    state,
    city,
    reading_time,
    COUNT(DISTINCT station) AS stations_reporting,
    ROUND(AVG(CASE WHEN pollutant = 'pm25' THEN avg_value END), 1) AS pm25,
    ROUND(AVG(CASE WHEN pollutant = 'pm10' THEN avg_value END), 1) AS pm10,
    ROUND(AVG(CASE WHEN pollutant = 'no2' THEN avg_value END), 1) AS no2,
    ROUND(AVG(CASE WHEN pollutant = 'so2' THEN avg_value END), 1) AS so2,
    ROUND(AVG(CASE WHEN pollutant = 'co' THEN avg_value END), 1) AS co,
    ROUND(AVG(CASE WHEN pollutant = 'o3' THEN avg_value END), 1) AS o3,
    ROUND(AVG(CASE WHEN pollutant = 'nh3' THEN avg_value END), 1) AS nh3
FROM workspace.aqi.silver_live_aqi
GROUP BY state, city, reading_time;