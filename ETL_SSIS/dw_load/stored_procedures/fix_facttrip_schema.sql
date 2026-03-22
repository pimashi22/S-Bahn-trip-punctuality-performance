-- ============================================================
-- File:    05_fix_facttrip_schema.sql
-- Purpose: Pre-load schema fixes required on FactTrip before
--          the SSIS Data Flow can successfully bulk-insert.
--          Run ONCE before executing the SSIS package.
-- Database: BerlinSBahn_DW
-- Author:   BerlinSBahn DW Project
-- Date:     2026-03-22
-- ============================================================

USE BerlinSBahn_DW;
GO

-- ============================================================
-- FIX 1: Allow NULL on weather_key
--        Some trips have no matching weather record.
--        The LEFT JOIN in SSIS returns NULL; the column must
--        accept it.
-- ============================================================
ALTER TABLE dbo.FactTrip
    ALTER COLUMN weather_key INT NULL;
GO

-- ============================================================
-- FIX 2: Allow NULL on trip_duration_cat
--        This column is not populated by the SSIS pipeline
--        (no source data available at load time).
-- ============================================================
ALTER TABLE dbo.FactTrip
    ALTER COLUMN trip_duration_cat VARCHAR(50) NULL;
GO

-- ============================================================
-- FIX 3: Disable the incident_key FK constraint
--        All trips load with incident_key = NULL because
--        incident matching is out of scope for this pipeline.
--        The FK is disabled (not dropped) to preserve the
--        schema design intent.
-- ============================================================
ALTER TABLE dbo.FactTrip
    NOCHECK CONSTRAINT FK__FactTrip__incide__01142BA1;
GO

-- ============================================================
-- VERIFY: Confirm nullable columns
-- ============================================================
SELECT
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'FactTrip'
  AND COLUMN_NAME IN ('weather_key', 'trip_duration_cat', 'incident_key')
ORDER BY ORDINAL_POSITION;
GO
