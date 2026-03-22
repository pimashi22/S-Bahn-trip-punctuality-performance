-- ============================================================
-- File:    05_prepare_facttrip_view.sql
-- Purpose: Create/alter staging view for FactTrip ETL
--          Pre-computes all derived columns and type casts
--          required by the SSIS Data Flow for FactTrip.
-- Database: BerlinSBahn_Staging
-- Author:   BerlinSBahn DW Project
-- Date:     2026-03-22
-- ============================================================

USE BerlinSBahn_Staging;
GO

-- ============================================================
-- STEP 1: Create or alter the prepared view
-- ============================================================
IF OBJECT_ID('dbo.vw_stg_trips_prepared', 'V') IS NOT NULL
    DROP VIEW dbo.vw_stg_trips_prepared;
GO

CREATE VIEW vw_stg_trips_prepared AS
SELECT
    -- Primary key (explicit INT cast)
    CAST(trip_id AS INT) AS trip_id,

    -- Foreign key source columns for SSIS Lookup transforms
    line_id,
    start_station_id,
    end_station_id,

    -- Departure datetime
    CAST(scheduled_departure_time AS DATETIME) AS scheduled_departure,

    -- Delay in minutes (explicit INT cast)
    CAST(delay_minutes AS INT) AS delay_minutes,

    -- Boolean flags: convert varchar 'True'/'False' → BIT
    CASE WHEN is_delayed   = 'True' THEN CAST(1 AS BIT) ELSE CAST(0 AS BIT) END AS is_delayed,
    CASE WHEN is_cancelled = 'True' THEN CAST(1 AS BIT) ELSE CAST(0 AS BIT) END AS is_cancelled,
    CASE WHEN is_peak_hour = 'True' THEN CAST(1 AS BIT) ELSE CAST(0 AS BIT) END AS is_peak_hour,

    -- Date key: integer YYYYMMDD for DimDate lookup
    CAST(FORMAT(CAST(scheduled_departure_time AS DATETIME), 'yyyyMMdd') AS INT) AS date_key_val,

    -- Weather timestamp: truncated to hour for DimWeather lookup
    DATEADD(HOUR, DATEDIFF(HOUR, 0, CAST(scheduled_departure_time AS DATETIME)), 0) AS weather_timestamp,

    -- Delay category
    CASE
        WHEN is_cancelled = 'True'              THEN 'Cancelled'
        WHEN delay_minutes = 0                  THEN 'None'
        WHEN CAST(delay_minutes AS INT) <= 5    THEN 'Minor'
        WHEN CAST(delay_minutes AS INT) <= 15   THEN 'Moderate'
        ELSE 'Severe'
    END AS delay_category,

    -- Time of day bucket
    CASE
        WHEN DATEPART(HOUR, CAST(scheduled_departure_time AS DATETIME)) BETWEEN 6  AND 11 THEN 'Morning'
        WHEN DATEPART(HOUR, CAST(scheduled_departure_time AS DATETIME)) BETWEEN 12 AND 17 THEN 'Afternoon'
        WHEN DATEPART(HOUR, CAST(scheduled_departure_time AS DATETIME)) BETWEEN 18 AND 21 THEN 'Evening'
        ELSE 'Night'
    END AS time_of_day,

    -- Performance score
    CASE
        WHEN is_cancelled = 'True'              THEN 0
        WHEN delay_minutes = 0                  THEN 100
        WHEN CAST(delay_minutes AS INT) <= 5    THEN 75
        WHEN CAST(delay_minutes AS INT) <= 15   THEN 50
        ELSE 25
    END AS performance_score

FROM dbo.stg_trips;
GO

-- ============================================================
-- STEP 2: Quick sanity check after creation
-- ============================================================
SELECT TOP 5 * FROM dbo.vw_stg_trips_prepared;
GO
