-- ============================================================
-- FILE: 02_verify_staging.sql
-- PURPOSE: Verify all staging tables loaded correctly
-- Run after BerlinSBahn_Load_Staging.dtsx executes
-- ============================================================

USE BerlinSBahn_Staging;

-- Row counts for all staging tables
SELECT 'stg_lines'              AS table_name,
        COUNT(*)                AS row_count
FROM stg_lines
UNION ALL
SELECT 'stg_stations',          COUNT(*) FROM stg_stations
UNION ALL
SELECT 'stg_incidents',         COUNT(*) FROM stg_incidents
UNION ALL
SELECT 'stg_weather_kaggle',    COUNT(*) FROM stg_weather_kaggle
UNION ALL
SELECT 'stg_trips',             COUNT(*) FROM stg_trips
UNION ALL
SELECT 'stg_weather_openmeteo', COUNT(*) FROM stg_weather_openmeteo;

-- Expected results:
-- stg_lines                  6
-- stg_stations              10
-- stg_incidents             36
-- stg_weather_kaggle      8761
-- stg_trips             131771
-- stg_weather_openmeteo   8784

-- Sample data checks
SELECT TOP 3 * FROM stg_lines;
SELECT TOP 3 * FROM stg_stations;
SELECT TOP 3 * FROM stg_incidents;
SELECT TOP 3 * FROM stg_weather_kaggle;
SELECT TOP 3 * FROM stg_trips;
SELECT TOP 3 * FROM stg_weather_openmeteo;
