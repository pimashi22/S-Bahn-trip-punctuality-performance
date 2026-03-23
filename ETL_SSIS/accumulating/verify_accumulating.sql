-- ============================================
-- Task 06: Verify Accumulating Fact Update
-- Database: BerlinSBahn_DW
-- Run AFTER executing the SSIS package
-- ============================================

USE BerlinSBahn_DW;
GO

-- 1. View all updated rows
SELECT
    trip_id,
    accm_txn_create_time,
    accm_txn_complete_time,
    txn_process_time_hours
FROM dbo.FactTrip
WHERE accm_txn_complete_time IS NOT NULL
ORDER BY trip_id;
GO

-- 2. Count updated vs pending
SELECT
    COUNT(*)                                                        AS total_rows,
    SUM(CASE WHEN accm_txn_complete_time IS NOT NULL THEN 1 ELSE 0 END) AS completed_rows,
    SUM(CASE WHEN accm_txn_complete_time IS NULL     THEN 1 ELSE 0 END) AS pending_rows
FROM dbo.FactTrip;
GO

-- 3. Validate txn_process_time_hours calculation
SELECT TOP 10
    trip_id,
    accm_txn_create_time,
    accm_txn_complete_time,
    txn_process_time_hours                                              AS stored_hours,
    DATEDIFF(HOUR, accm_txn_create_time, accm_txn_complete_time)        AS expected_hours,
    CASE
        WHEN txn_process_time_hours =
             DATEDIFF(HOUR, accm_txn_create_time, accm_txn_complete_time)
        THEN 'CORRECT'
        ELSE 'MISMATCH'
    END                                                                 AS validation_status
FROM dbo.FactTrip
WHERE accm_txn_complete_time IS NOT NULL;
GO
