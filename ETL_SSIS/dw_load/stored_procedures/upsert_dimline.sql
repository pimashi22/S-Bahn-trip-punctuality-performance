-- ============================================================
-- FILE: 01_upsert_dimline.sql
-- PURPOSE: Stored procedure to upsert data into DimLine
-- Database: BerlinSBahn_DW
-- Called by: SSIS Package BerlinSBahn_Load_DW.dtsx
-- ============================================================

USE BerlinSBahn_DW;
GO

CREATE PROCEDURE dbo.UpsertDimLine
    @line_id          VARCHAR(10),
    @line_name        VARCHAR(20),
    @is_ring_line     VARCHAR(10),
    @delay_propensity FLOAT
AS
BEGIN
    DECLARE @is_ring_bit BIT = 
        CASE WHEN @is_ring_line = 'True' THEN 1 ELSE 0 END

    DECLARE @line_category VARCHAR(20) = 
        CASE WHEN @is_ring_line = 'True' 
             THEN 'Ring' ELSE 'Suburban' END

    IF NOT EXISTS (
        SELECT line_key FROM dbo.DimLine 
        WHERE line_id = @line_id
    )
    BEGIN
        INSERT INTO dbo.DimLine 
        (line_id, line_name, is_ring_line, 
         delay_propensity, line_category,
         insert_date, modified_date)
        VALUES 
        (@line_id, @line_name, @is_ring_bit,
         @delay_propensity, @line_category,
         GETDATE(), GETDATE())
    END
    ELSE
    BEGIN
        UPDATE dbo.DimLine
        SET line_name        = @line_name,
            is_ring_line     = @is_ring_bit,
            delay_propensity = @delay_propensity,
            line_category    = @line_category,
            modified_date    = GETDATE()
        WHERE line_id = @line_id
    END
END;
GO
