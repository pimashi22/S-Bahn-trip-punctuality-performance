
-- ============================================================
-- Task 2 - Source Type 2: SQL Server Database
-- Creates BerlinSBahn_Source and staging table
-- ============================================================

USE BerlinSBahn_Source;

DROP TABLE IF EXISTS dbo.stg_weather_openmeteo;

CREATE TABLE dbo.stg_weather_openmeteo (
    timestamp         DATETIME      NOT NULL,
    temperature_c     FLOAT         NOT NULL,
    precipitation_mm  FLOAT         NOT NULL,
    wind_speed_kmh    FLOAT         NOT NULL,
    weather_condition VARCHAR(50)   NOT NULL
);
