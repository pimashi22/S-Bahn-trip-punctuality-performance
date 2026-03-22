-- ============================================================
-- FILE: 04_verify_dimincident.sql
-- PURPOSE: Verify DimIncident loaded correctly
-- Run after BerlinSBahn_Load_DW.dtsx executes
-- ============================================================

USE BerlinSBahn_DW;

-- Row count (should be 36)
SELECT COUNT(*) AS row_count FROM DimIncident;

-- Full data check
SELECT TOP 5
    incident_key,
    incident_id,
    incident_type,
    line_key,
    incident_date,
    incident_hour,
    severity_category,
    delay_impact_factor
FROM DimIncident
ORDER BY incident_key;

-- Check severity distribution
SELECT
    severity_category,
    COUNT(*) AS row_count
FROM DimIncident
GROUP BY severity_category;
-- Expected: High=9, Low=11, Medium=16

-- Check incident types
SELECT
    incident_type,
    COUNT(*) AS row_count
FROM DimIncident
GROUP BY incident_type;

-- Verify line_key was resolved correctly (Lookup worked)
SELECT
    i.incident_id,
    i.incident_type,
    i.line_key,
    l.line_id,
    i.severity_category
FROM DimIncident i
JOIN DimLine l ON i.line_key = l.line_key
ORDER BY i.incident_key;

-- Incidents by line
SELECT
    l.line_id,
    COUNT(*) AS incident_count
FROM DimIncident i
JOIN DimLine l ON i.line_key = l.line_key
GROUP BY l.line_id
ORDER BY incident_count DESC;
