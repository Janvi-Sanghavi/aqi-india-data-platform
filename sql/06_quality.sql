-- =====================================================
-- 06_quality: Data quality and pipeline monitoring
-- The checks themselves run automatically from:
--   sql/pipeline/03_quality_checks.sql
-- =====================================================

-- Setup (run once): table that records every pipeline run
CREATE TABLE IF NOT EXISTS workspace.aqi.pipeline_runs (
    run_id STRING,
    run_started_utc TIMESTAMP,
    run_finished_utc TIMESTAMP,
    status STRING,
    records_fetched INT,
    files_loaded INT,
    readings_inserted INT,
    checks_failed INT,
    message STRING
);

-- Setup (run once): table that records every quality check result
CREATE TABLE IF NOT EXISTS workspace.aqi.data_quality_results (
    run_id STRING,
    check_name STRING,
    check_value DOUBLE,
    threshold STRING,
    passed BOOLEAN,
    severity STRING,
    checked_at_utc TIMESTAMP
);

-- Monitoring: pipeline run history (newest first)
SELECT run_started_utc, status, records_fetched, files_loaded,
       readings_inserted, checks_failed, message
FROM workspace.aqi.pipeline_runs
ORDER BY run_started_utc DESC;

-- Monitoring: quality check results from the latest run
SELECT check_name, check_value, threshold, passed, severity
FROM workspace.aqi.data_quality_results
WHERE run_id = (SELECT run_id FROM workspace.aqi.pipeline_runs
                ORDER BY run_started_utc DESC LIMIT 1)
ORDER BY severity, check_name;