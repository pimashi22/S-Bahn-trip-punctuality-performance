-- ============================================================
-- FILE: 04_upsert_dimincident.sql
-- PURPOSE: Stored procedure to upsert data into DimIncident
-- Database: BerlinSBahn_DW
-- Called by: SSIS Package BerlinSBahn_Load_DW.dtsx
-- ============================================================

USE BerlinSBahn_DW;
GO

ALTER PROCEDURE dbo.UpsertDimIncident
    @incident_id         VARCHAR(20),
    @incident_type       VARCHAR(50),
    @delay_impact_factor FLOAT,
    @line_key            INT,
    @incident_timestamp  VARCHAR(30)
AS
BEGIN
    DECLARE @inc_date DATE =
        CAST(CAST(@incident_timestamp AS DATETIME) AS DATE)

    DECLARE @inc_hour INT =
        DATEPART(HOUR, CAST(@incident_timestamp AS DATETIME))

    DECLARE @severity VARCHAR(10) =
        CASE
            WHEN @delay_impact_factor <= 2  THEN 'Low'
            WHEN @delay_impact_factor <= 5  THEN 'Medium'
            ELSE 'High'
        END

    IF NOT EXISTS (
        SELECT incident_key FROM dbo.DimIncident
        WHERE incident_id = @incident_id
    )
    BEGIN
        INSERT INTO dbo.DimIncident
        (incident_id, incident_type, delay_impact_factor,
         line_key, incident_date, incident_hour,
         severity_category, insert_date, modified_date)
        VALUES
        (@incident_id, @incident_type, @delay_impact_factor,
         @line_key, @inc_date, @inc_hour,
         @severity, GETDATE(), GETDATE())
    END
    ELSE
    BEGIN
        UPDATE dbo.DimIncident
        SET incident_type       = @incident_type,
            delay_impact_factor = @delay_impact_factor,
            line_key            = @line_key,
            incident_date       = @inc_date,
            incident_hour       = @inc_hour,
            severity_category   = @severity,
            modified_date       = GETDATE()
        WHERE incident_id = @incident_id
    END
END;
GO
