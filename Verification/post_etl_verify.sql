-- ============================================================
-- Verification - Task 5+6: After SSIS ETL loads
-- Run after all SSIS packages complete successfully
-- ============================================================

USE BerlinSBahn_DW;

-- Row counts for all tables
SELECT 'DimDate'     AS table_name, COUNT(*) AS row_count
FROM DimDate
UNION ALL
SELECT 'DimLine',     COUNT(*) FROM DimLine
UNION ALL
SELECT 'DimStation',  COUNT(*) FROM DimStation
UNION ALL
SELECT 'DimWeather',  COUNT(*) FROM DimWeather
UNION ALL
SELECT 'DimIncident', COUNT(*) FROM DimIncident
UNION ALL
SELECT 'FactTrip',    COUNT(*) FROM FactTrip;

-- Expected results:
-- DimDate      366
-- DimLine        6
-- DimStation    10
-- DimWeather  8784
-- DimIncident   36
-- FactTrip  131771

-- Sample FactTrip data
SELECT TOP 10 * FROM FactTrip;

-- Delay category distribution
SELECT delay_category,
       COUNT(*) AS trip_count
FROM FactTrip
GROUP BY delay_category
ORDER BY trip_count DESC;

-- Accumulating fact verification (Task 6)
SELECT TOP 10
    trip_key,
    accm_txn_create_time,
    accm_txn_complete_time,
    txn_process_time_hours
FROM FactTrip
WHERE accm_txn_complete_time IS NOT NULL;
