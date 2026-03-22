-- ============================================================
-- FILE: 03_verify_dimweather.sql
-- PURPOSE: Verify DimWeather loaded correctly from both sources
-- Run after BerlinSBahn_Load_DW.dtsx executes
-- ============================================================

USE BerlinSBahn_DW;

-- Total row count (should be ~8784 unique timestamps)
SELECT COUNT(*) AS total_rows FROM DimWeather;

-- Check both sources loaded
SELECT
    data_source,
    COUNT(*) AS row_count
FROM DimWeather
GROUP BY data_source;
-- Expected:
-- Open-Meteo   ~23 (unique timestamps not in Kaggle)
-- Kaggle       ~8761

-- Check temp categories distribution
SELECT
    temp_category,
    COUNT(*) AS row_count
FROM DimWeather
GROUP BY temp_category;
-- Expected: Cold, Mild, Warm, Hot categories

-- Sample data from both sources
SELECT TOP 5
    weather_key,
    timestamp,
    temperature_c,
    weather_condition,
    temp_category,
    data_source
FROM DimWeather
ORDER BY weather_key;

-- Verify temperature ranges per category
SELECT
    temp_category,
    MIN(temperature_c) AS min_temp,
    MAX(temperature_c) AS max_temp,
    COUNT(*) AS count
FROM DimWeather
GROUP BY temp_category
ORDER BY min_temp;
