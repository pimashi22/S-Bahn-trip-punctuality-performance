-- ============================================================
-- Verification - Task 4: DW Tables
-- ============================================================

USE BerlinSBahn_DW;

-- Table structure check
SELECT
    t.TABLE_NAME,
    COUNT(c.COLUMN_NAME) AS column_count
FROM INFORMATION_SCHEMA.TABLES t
JOIN INFORMATION_SCHEMA.COLUMNS c
    ON t.TABLE_NAME = c.TABLE_NAME
WHERE t.TABLE_TYPE = 'BASE TABLE'
GROUP BY t.TABLE_NAME
ORDER BY t.TABLE_NAME;

-- DimDate checks
SELECT COUNT(*)  AS total_days       FROM DimDate;
SELECT COUNT(*)  AS weekend_days
FROM DimDate     WHERE is_weekend       = 1;
SELECT COUNT(*)  AS public_holidays
FROM DimDate     WHERE is_public_holiday = 1;

sql-- ============================================================
-- FILE: 02_dw_verify.sql
-- PURPOSE: Verify DW tables structure
-- Run after Task 4 CREATE TABLE scripts
-- ============================================================

USE BerlinSBahn_DW;

-- Table column counts
SELECT
    t.TABLE_NAME,
    COUNT(c.COLUMN_NAME) AS column_count
FROM INFORMATION_SCHEMA.TABLES t
JOIN INFORMATION_SCHEMA.COLUMNS c
    ON t.TABLE_NAME = c.TABLE_NAME
WHERE t.TABLE_TYPE = 'BASE TABLE'
GROUP BY t.TABLE_NAME
ORDER BY t.TABLE_NAME;

-- Expected:
-- DimDate      14
-- DimIncident  10
-- DimLine       8
-- DimStation   10
-- DimWeather   10
-- FactTrip     22

-- DimDate verification
SELECT COUNT(*)  AS total_days        FROM DimDate;
SELECT COUNT(*)  AS weekend_days
FROM DimDate WHERE is_weekend       = 1;
SELECT COUNT(*)  AS public_holidays
FROM DimDate WHERE is_public_holiday = 1;

-- Expected:
-- total_days     = 366
-- weekend_days   = 104
-- public_holidays = 9
```
