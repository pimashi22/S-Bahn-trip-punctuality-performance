-- ============================================================
-- FILE: 03_upsert_dimweather.sql
-- PURPOSE: Stored procedure to upsert data into DimWeather
-- Database: BerlinSBahn_DW
-- Called by: SSIS Package BerlinSBahn_Load_DW.dtsx
-- ============================================================

USE BerlinSBahn_DW;
GO

ALTER PROCEDURE dbo.UpsertDimWeather
    @timestamp         VARCHAR(30),
    @temperature_c     FLOAT,
    @precipitation_mm  FLOAT,
    @wind_speed_kmh    FLOAT,
    @weather_condition VARCHAR(50),
    @data_source       NVARCHAR(30)
AS
BEGIN
    DECLARE @ts DATETIME = CAST(@timestamp AS DATETIME)

    DECLARE @temp_category NVARCHAR(10) =
        CASE
            WHEN @temperature_c < 5   THEN 'Cold'
            WHEN @temperature_c < 15  THEN 'Mild'
            WHEN @temperature_c < 25  THEN 'Warm'
            ELSE 'Hot'
        END

    IF NOT EXISTS (
        SELECT weather_key FROM dbo.DimWeather
        WHERE timestamp = @ts
    )
    BEGIN
        INSERT INTO dbo.DimWeather
        (timestamp, temperature_c, precipitation_mm,
         wind_speed_kmh, weather_condition,
         temp_category, data_source,
         insert_date, modified_date)
        VALUES
        (@ts, @temperature_c, @precipitation_mm,
         @wind_speed_kmh, @weather_condition,
         @temp_category, @data_source,
         GETDATE(), GETDATE())
    END
    ELSE
    BEGIN
        UPDATE dbo.DimWeather
        SET temperature_c     = @temperature_c,
            precipitation_mm  = @precipitation_mm,
            wind_speed_kmh    = @wind_speed_kmh,
            weather_condition = @weather_condition,
            temp_category     = @temp_category,
            data_source       = @data_source,
            modified_date     = GETDATE()
        WHERE timestamp = @ts
    END
END;
GO
