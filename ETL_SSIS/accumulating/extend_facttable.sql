-- ============================================
-- Task 06: Extend FactTrip with Accumulating Columns
-- Database: BerlinSBahn_DW
-- Run ONCE before executing the SSIS package
-- ============================================

USE BerlinSBahn_DW;
GO

-- Step 1: Add 3 accumulating columns to FactTrip
ALTER TABLE dbo.FactTrip
ADD accm_txn_create_time   DATETIME NULL,
    accm_txn_complete_time  DATETIME NULL,
    txn_process_time_hours  INT      NULL;
GO

-- Step 2: Populate accm_txn_create_time = system time at load
-- (Represents when the fact row was originally loaded)
UPDATE dbo.FactTrip
SET accm_txn_create_time = GETDATE()
WHERE accm_txn_create_time IS NULL;
GO

-- Step 3: Fix column type to INT for SSIS DT_I4 compatibility
-- (DATEDIFF returns INT, so column must be INT not FLOAT)
ALTER TABLE dbo.FactTrip
ALTER COLUMN txn_process_time_hours INT;
GO

-- Step 4: Verify columns added correctly
SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'FactTrip'
AND COLUMN_NAME IN (
    'accm_txn_create_time',
    'accm_txn_complete_time',
    'txn_process_time_hours'
);
GO

-- Step 5: Confirm accm_txn_create_time is populated
SELECT TOP 5
    trip_id,
    accm_txn_create_time,
    accm_txn_complete_time,
    txn_process_time_hours
FROM dbo.FactTrip;
GO
