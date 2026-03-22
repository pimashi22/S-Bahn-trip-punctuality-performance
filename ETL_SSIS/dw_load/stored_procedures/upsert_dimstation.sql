-- PURPOSE: SCD Type 2 is handled by SSIS SCD Wizard
--          This file documents the DimStation table structure
--          and the OLE DB Command used to expire old records
-- Database: BerlinSBahn_DW
-- Called by: SSIS Package BerlinSBahn_Load_DW.dtsx
-- ============================================================
 
USE BerlinSBahn_DW;
GO
 
-- The SCD Wizard auto-generates the following logic:
-- 1. New records: INSERT via Insert Destination component
-- 2. Changed historical attributes: 
--    - Old record: effective_end_date filled, is_current = 0
--    - New record: INSERT with new effective_start_date
-- 3. Changed Type 1 attributes: UPDATE in place via OLE DB Command 1
 
-- The OLE DB Command (left branch of SCD) uses this SQL
-- to expire old records when a historical attribute changes:
-- UPDATE DimStation
-- SET effective_end_date = ?,
--     is_current = 0
-- WHERE station_id = ? AND effective_end_date IS NULL
