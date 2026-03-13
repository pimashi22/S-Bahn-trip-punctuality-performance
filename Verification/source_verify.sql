
-- ============================================================
-- Verification - Task 2: Source Data
-- ============================================================

USE BerlinSBahn_Source;

SELECT 'stg_weather_openmeteo'  AS table_name,
       COUNT(*)                  AS row_count
FROM dbo.stg_weather_openmeteo;

SELECT MIN(timestamp) AS earliest,
       MAX(timestamp) AS latest
FROM dbo.stg_weather_openmeteo;

SELECT TOP 5 * FROM dbo.stg_weather_openmeteo;
