-- ============================================================
-- FILE: 01_verify_dimline.sql
-- PURPOSE: Verify DimLine loaded correctly
-- Run after BerlinSBahn_Load_DW.dtsx executes
-- ============================================================

USE BerlinSBahn_DW;

-- Row count (should be 6)
SELECT COUNT(*) AS row_count FROM DimLine;

-- Full data check
SELECT 
    line_key,
    line_id,
    line_name,
    is_ring_line,
    delay_propensity,
    line_category,
    insert_date,
    modified_date
FROM DimLine
ORDER BY line_key;

-- Verify ring lines
SELECT 
    line_id,
    line_category,
    is_ring_line
FROM DimLine
WHERE is_ring_line = 1;
-- Should return S41 and S42 only

-- Verify suburban lines
SELECT 
    line_id,
    line_category,
    is_ring_line
FROM DimLine
WHERE is_ring_line = 0;
-- Should return S1, S2, S5, S7
