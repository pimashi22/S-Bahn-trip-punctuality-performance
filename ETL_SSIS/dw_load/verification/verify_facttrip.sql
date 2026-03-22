-- ============================================================
-- File:    05_verify_facttrip.sql
-- Purpose: Post-load verification queries for FactTrip.
--          Run after the SSIS package completes successfully.
-- Database: BerlinSBahn_DW
-- Author:   BerlinSBahn DW Project
-- Date:     2026-03-22
-- ============================================================

USE BerlinSBahn_DW;
GO

-- ============================================================
-- CHECK 1: Total row count (expected: 131,771)
-- ============================================================
SELECT COUNT(*) AS total_rows FROM dbo.FactTrip;
GO

-- ============================================================
-- CHECK 2: NULL audit on all key columns
--          line_key, station keys, date_key must be 0
--          weather_key and incident_key NULLs are expected
-- ============================================================
SELECT
    COUNT(*)  AS total_rows,
    SUM(CASE WHEN line_key          IS NULL THEN 1 ELSE 0 END) AS null_line_keys,
    SUM(CASE WHEN start_station_key IS NULL THEN 1 ELSE 0 END) AS null_start_stations,
    SUM(CASE WHEN end_station_key   IS NULL THEN 1 ELSE 0 END) AS null_end_stations,
    SUM(CASE WHEN date_key          IS NULL THEN 1 ELSE 0 END) AS null_date_keys,
    SUM(CASE WHEN weather_key       IS NULL THEN 1 ELSE 0 END) AS null_weather_keys,
    SUM(CASE WHEN incident_key      IS NULL THEN 1 ELSE 0 END) AS null_incident_keys
FROM dbo.FactTrip;
GO

-- ============================================================
-- CHECK 3: Row distribution by delay_category
-- ============================================================
SELECT
    delay_category,
    COUNT(*) AS trip_count,
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS DECIMAL(5,2)) AS pct
FROM dbo.FactTrip
GROUP BY delay_category
ORDER BY trip_count DESC;
GO

-- ============================================================
-- CHECK 4: Row distribution by time_of_day
-- ============================================================
SELECT
    time_of_day,
    COUNT(*) AS trip_count
FROM dbo.FactTrip
GROUP BY time_of_day
ORDER BY trip_count DESC;
GO

-- ============================================================
-- CHECK 5: Average performance score overall
-- ============================================================
SELECT
    AVG(CAST(performance_score AS FLOAT)) AS avg_performance_score,
    MIN(performance_score) AS min_score,
    MAX(performance_score) AS max_score
FROM dbo.FactTrip;
GO

-- ============================================================
-- CHECK 6: FK integrity — confirm all keys exist in dims
-- ============================================================
-- Line key
SELECT COUNT(*) AS unmatched_line_keys
FROM dbo.FactTrip f
LEFT JOIN dbo.DimLine l ON f.line_key = l.line_key
WHERE l.line_key IS NULL;

-- Station keys
SELECT COUNT(*) AS unmatched_start_station_keys
FROM dbo.FactTrip f
LEFT JOIN dbo.DimStation s ON f.start_station_key = s.station_key
WHERE s.station_key IS NULL;

SELECT COUNT(*) AS unmatched_end_station_keys
FROM dbo.FactTrip f
LEFT JOIN dbo.DimStation s ON f.end_station_key = s.station_key
WHERE s.station_key IS NULL;

-- Date key
SELECT COUNT(*) AS unmatched_date_keys
FROM dbo.FactTrip f
LEFT JOIN dbo.DimDate d ON f.date_key = d.date_key
WHERE d.date_key IS NULL;
GO

-- ============================================================
-- CHECK 7: Preview sample rows
-- ============================================================
SELECT TOP 10 * FROM dbo.FactTrip ORDER BY trip_key;
GO
