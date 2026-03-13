-- ============================================================
-- Task 2 - Load Open-Meteo weather data into SQL Server
-- File must be at C:\BerlinSBahn\weather_openmeteo.csv
-- ============================================================

USE BerlinSBahn_Source;

BULK INSERT dbo.stg_weather_openmeteo
FROM 'C:\BerlinSBahn\weather_openmeteo.csv'
WITH (
    FIRSTROW        = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR   = '0x0a',
    CODEPAGE        = '65001',
    TABLOCK
);

-- Verify
SELECT COUNT(*)           AS total_rows  FROM dbo.stg_weather_openmeteo;
SELECT TOP 5 *                           FROM dbo.stg_weather_openmeteo;
SELECT MIN(timestamp)     AS earliest,
       MAX(timestamp)     AS latest      FROM dbo.stg_weather_openmeteo;
