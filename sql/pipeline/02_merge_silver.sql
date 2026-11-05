MERGE INTO workspace.aqi.silver_live_aqi AS target
USING (
  SELECT
    r.state,
    r.city,
    r.station,
    TO_TIMESTAMP(r.last_update, 'dd-MM-yyyy HH:mm:ss') AS reading_time,
    CAST(r.latitude AS DOUBLE)  AS latitude,
    CAST(r.longitude AS DOUBLE) AS longitude,
    CASE r.pollutant_id
      WHEN 'PM2.5' THEN 'pm25'
      WHEN 'PM10'  THEN 'pm10'
      WHEN 'NO2'   THEN 'no2'
      WHEN 'SO2'   THEN 'so2'
      WHEN 'CO'    THEN 'co'
      WHEN 'OZONE' THEN 'o3'
      WHEN 'NH3'   THEN 'nh3'
    END AS pollutant,
    TRY_CAST(NULLIF(r.min_value, 'NA') AS DOUBLE) AS min_value,
    TRY_CAST(NULLIF(r.max_value, 'NA') AS DOUBLE) AS max_value,
    TRY_CAST(NULLIF(r.avg_value, 'NA') AS DOUBLE) AS avg_value,
    CAST(b.fetched_at_utc AS TIMESTAMP) AS fetched_at_utc,
    b.source_file
  FROM workspace.aqi.bronze_live_raw b
  LATERAL VIEW EXPLODE(b.records) AS r
  WHERE b.source_file NOT IN (SELECT DISTINCT source_file FROM workspace.aqi.silver_live_aqi)
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY r.station, r.pollutant_id, r.last_update
    ORDER BY b.fetched_at_utc DESC
  ) = 1
) AS source
ON  target.station      = source.station
AND target.pollutant    = source.pollutant
AND target.reading_time = source.reading_time
WHEN NOT MATCHED THEN INSERT *