
-- Step 3: Data quality checks on the latest file loaded into silver
WITH latest AS (
  SELECT *
  FROM workspace.aqi.silver_live_aqi
  WHERE source_file = (SELECT MAX(source_file) FROM workspace.aqi.silver_live_aqi)
)
SELECT 'row_count' AS check_name,
       CAST(COUNT(*) AS DOUBLE) AS check_value,
       '>= 1000' AS threshold,
       COUNT(*) >= 1000 AS passed,
       'critical' AS severity
FROM latest

UNION ALL
SELECT 'missing_avg_pct',
       ROUND(SUM(CASE WHEN avg_value IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2),
       '<= 25',
       SUM(CASE WHEN avg_value IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) <= 25,
       'warning'
FROM latest

UNION ALL
SELECT 'negative_values',
       CAST(SUM(CASE WHEN avg_value < 0 THEN 1 ELSE 0 END) AS DOUBLE),
       '= 0',
       SUM(CASE WHEN avg_value < 0 THEN 1 ELSE 0 END) = 0,
       'warning'
FROM latest

UNION ALL
SELECT 'unknown_pollutant',
       CAST(SUM(CASE WHEN pollutant IS NULL THEN 1 ELSE 0 END) AS DOUBLE),
       '= 0',
       SUM(CASE WHEN pollutant IS NULL THEN 1 ELSE 0 END) = 0,
       'warning'
FROM latest

UNION ALL
SELECT 'duplicate_readings',
       CAST(COUNT(*) AS DOUBLE),
       '= 0',
       COUNT(*) = 0,
       'critical'
FROM (
  SELECT station, pollutant, reading_time
  FROM workspace.aqi.silver_live_aqi
  GROUP BY station, pollutant, reading_time
  HAVING COUNT(*) > 1
)

UNION ALL
SELECT 'data_age_hours',
       ROUND((UNIX_TIMESTAMP(FROM_UTC_TIMESTAMP(MAX(fetched_at_utc), 'Asia/Kolkata'))
              - UNIX_TIMESTAMP(MAX(reading_time))) / 3600.0, 2),
       '<= 3',
       (UNIX_TIMESTAMP(FROM_UTC_TIMESTAMP(MAX(fetched_at_utc), 'Asia/Kolkata'))
        - UNIX_TIMESTAMP(MAX(reading_time))) / 3600.0 <= 3,
       'warning'
FROM latest;