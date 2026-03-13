-- ============================================================
-- Task 4 - Populate DimDate with all 366 days of 2024
-- Berlin public holidays included
-- Run BEFORE SSIS packages
-- ============================================================

USE BerlinSBahn_DW;

DECLARE @StartDate DATE = '2024-01-01';
DECLARE @EndDate   DATE = '2024-12-31';
DECLARE @Date      DATE = @StartDate;

WHILE @Date <= @EndDate
BEGIN
    INSERT INTO DimDate (
        date_key, full_date, day_of_week, day_number,
        month_number, month_name, quarter, year,
        season, is_weekend, is_public_holiday, holiday_name
    )
    VALUES (
        CAST(FORMAT(@Date,'yyyyMMdd') AS INT),
        @Date,
        DATENAME(WEEKDAY, @Date),
        DAY(@Date),
        MONTH(@Date),
        DATENAME(MONTH, @Date),
        'Q' + CAST(DATEPART(QUARTER,@Date) AS VARCHAR),
        YEAR(@Date),
        CASE
            WHEN MONTH(@Date) IN (3,4,5)   THEN 'Spring'
            WHEN MONTH(@Date) IN (6,7,8)   THEN 'Summer'
            WHEN MONTH(@Date) IN (9,10,11) THEN 'Autumn'
            ELSE 'Winter'
        END,
        CASE WHEN DATENAME(WEEKDAY,@Date)
             IN ('Saturday','Sunday') THEN 1 ELSE 0 END,
        CASE
            WHEN @Date = '2024-01-01' THEN 1
            WHEN @Date = '2024-04-19' THEN 1
            WHEN @Date = '2024-04-21' THEN 1
            WHEN @Date = '2024-05-01' THEN 1
            WHEN @Date = '2024-05-09' THEN 1
            WHEN @Date = '2024-05-20' THEN 1
            WHEN @Date = '2024-10-03' THEN 1
            WHEN @Date = '2024-12-25' THEN 1
            WHEN @Date = '2024-12-26' THEN 1
            ELSE 0
        END,
        CASE
            WHEN @Date = '2024-01-01' THEN 'New Year Day'
            WHEN @Date = '2024-04-19' THEN 'Good Friday'
            WHEN @Date = '2024-04-21' THEN 'Easter Sunday'
            WHEN @Date = '2024-05-01' THEN 'Labour Day'
            WHEN @Date = '2024-05-09' THEN 'Ascension Day'
            WHEN @Date = '2024-05-20' THEN 'Whit Monday'
            WHEN @Date = '2024-10-03' THEN 'German Unity Day'
            WHEN @Date = '2024-12-25' THEN 'Christmas Day'
            WHEN @Date = '2024-12-26' THEN 'Boxing Day'
            ELSE NULL
        END
    );
    SET @Date = DATEADD(DAY, 1, @Date);
END;

SELECT COUNT(*) AS total_days     FROM DimDate;
SELECT * FROM DimDate WHERE is_public_holiday = 1;
