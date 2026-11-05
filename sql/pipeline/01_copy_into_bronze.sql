COPY INTO workspace.aqi.bronze_live_raw
FROM (
  SELECT
    fetched_at_utc,
    records,
    _metadata.file_name AS source_file
  FROM '/Volumes/workspace/aqi/bronze/live/'
)
FILEFORMAT = JSON
COPY_OPTIONS ('mergeSchema' = 'true')