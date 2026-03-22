-- Row count (should be 10 after initial load)
SELECT COUNT(*) AS row_count FROM DimStation;
 
-- Full data check
SELECT
    station_key,
    station_id,
    station_name,
    is_major_hub,
    location_category,
    effective_start_date,
    effective_end_date,
    is_current,
    insert_date,
    modified_date
FROM DimStation
ORDER BY station_key;
 
-- SCD Type 2 proof - shows both versions of S6
-- Run after updating S6 location_category in staging
SELECT
    station_key,
    station_id,
    station_name,
    location_category,
    effective_start_date,
    effective_end_date,
    is_current
FROM DimStation
WHERE station_id = 'S6'
ORDER BY station_key;
-- Expected:
-- Old record: effective_end_date filled, is_current = 0
-- New record: effective_end_date NULL,    is_current = 1
