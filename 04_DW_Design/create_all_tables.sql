
-- ============================================================
-- Task 4 - Create all DW tables in BerlinSBahn_DW
-- Run in this order - respects FK dependencies
-- ============================================================

USE BerlinSBahn_DW;

-- Drop in reverse FK order
DROP TABLE IF EXISTS FactTrip;
DROP TABLE IF EXISTS DimIncident;
DROP TABLE IF EXISTS DimWeather;
DROP TABLE IF EXISTS DimStation;
DROP TABLE IF EXISTS DimLine;
DROP TABLE IF EXISTS DimDate;

-- ── DimDate ─────────────────────────────────────────────────
CREATE TABLE DimDate (
    date_key            INT          NOT NULL PRIMARY KEY,
    full_date           DATE         NOT NULL,
    day_of_week         VARCHAR(10)  NOT NULL,
    day_number          INT          NOT NULL,
    month_number        INT          NOT NULL,
    month_name          VARCHAR(15)  NOT NULL,
    quarter             VARCHAR(5)   NOT NULL,
    year                INT          NOT NULL,
    season              VARCHAR(10)  NOT NULL,
    is_weekend          BIT          NOT NULL DEFAULT 0,
    is_public_holiday   BIT          NOT NULL DEFAULT 0,
    holiday_name        VARCHAR(50)  NULL,
    insert_date         DATETIME     NOT NULL DEFAULT GETDATE(),
    modified_date       DATETIME     NOT NULL DEFAULT GETDATE()
);

-- ── DimLine ─────────────────────────────────────────────────
CREATE TABLE DimLine (
    line_key            INT          NOT NULL IDENTITY(1,1) PRIMARY KEY,
    line_id             VARCHAR(10)  NOT NULL,
    line_name           VARCHAR(20)  NOT NULL,
    is_ring_line        BIT          NOT NULL DEFAULT 0,
    delay_propensity    FLOAT        NOT NULL,
    line_category       VARCHAR(20)  NOT NULL,
    insert_date         DATETIME     NOT NULL DEFAULT GETDATE(),
    modified_date       DATETIME     NOT NULL DEFAULT GETDATE()
);

-- ── DimStation (SCD Type 2) ──────────────────────────────────
CREATE TABLE DimStation (
    station_key             INT          NOT NULL IDENTITY(1,1) PRIMARY KEY,
    station_id              VARCHAR(10)  NOT NULL,
    station_name            VARCHAR(100) NOT NULL,
    is_major_hub            BIT          NOT NULL DEFAULT 0,
    location_category       VARCHAR(30)  NOT NULL,
    effective_start_date    DATE         NOT NULL DEFAULT GETDATE(),
    effective_end_date      DATE         NULL,
    is_current              BIT          NOT NULL DEFAULT 1,
    insert_date             DATETIME     NOT NULL DEFAULT GETDATE(),
    modified_date           DATETIME     NOT NULL DEFAULT GETDATE()
);

-- ── DimWeather ───────────────────────────────────────────────
CREATE TABLE DimWeather (
    weather_key         INT          NOT NULL IDENTITY(1,1) PRIMARY KEY,
    timestamp           DATETIME     NOT NULL,
    temperature_c       FLOAT        NOT NULL,
    precipitation_mm    FLOAT        NOT NULL,
    wind_speed_kmh      FLOAT        NOT NULL,
    weather_condition   VARCHAR(30)  NOT NULL,
    temp_category       VARCHAR(10)  NOT NULL,
    data_source         VARCHAR(30)  NOT NULL,
    insert_date         DATETIME     NOT NULL DEFAULT GETDATE(),
    modified_date       DATETIME     NOT NULL DEFAULT GETDATE()
);

-- ── DimIncident ──────────────────────────────────────────────
CREATE TABLE DimIncident (
    incident_key            INT          NOT NULL IDENTITY(1,1) PRIMARY KEY,
    incident_id             VARCHAR(20)  NOT NULL,
    incident_type           VARCHAR(50)  NOT NULL,
    delay_impact_factor     FLOAT        NOT NULL,
    line_key                INT          NOT NULL,
    incident_date           DATE         NOT NULL,
    incident_hour           INT          NOT NULL,
    severity_category       VARCHAR(10)  NOT NULL,
    insert_date             DATETIME     NOT NULL DEFAULT GETDATE(),
    modified_date           DATETIME     NOT NULL DEFAULT GETDATE(),
    FOREIGN KEY (line_key)  REFERENCES DimLine(line_key)
);

-- ── FactTrip ─────────────────────────────────────────────────
CREATE TABLE FactTrip (
    trip_key                INT          NOT NULL IDENTITY(1,1) PRIMARY KEY,
    trip_id                 INT          NOT NULL,
    line_key                INT          NOT NULL,
    start_station_key       INT          NOT NULL,
    end_station_key         INT          NOT NULL,
    date_key                INT          NOT NULL,
    weather_key             INT          NOT NULL,
    incident_key            INT          NULL,
    scheduled_departure     DATETIME     NOT NULL,
    delay_minutes           INT          NOT NULL DEFAULT 0,
    is_delayed              BIT          NOT NULL DEFAULT 0,
    is_cancelled            BIT          NOT NULL DEFAULT 0,
    is_peak_hour            BIT          NOT NULL DEFAULT 0,
    delay_category          VARCHAR(15)  NOT NULL,
    time_of_day             VARCHAR(15)  NOT NULL,
    trip_duration_cat       VARCHAR(15)  NOT NULL,
    performance_score       INT          NOT NULL,
    accm_txn_create_time    DATETIME     NULL,
    accm_txn_complete_time  DATETIME     NULL,
    txn_process_time_hours  FLOAT        NULL,
    insert_date             DATETIME     NOT NULL DEFAULT GETDATE(),
    modified_date           DATETIME     NOT NULL DEFAULT GETDATE(),
    FOREIGN KEY (line_key)
        REFERENCES DimLine(line_key),
    FOREIGN KEY (start_station_key)
        REFERENCES DimStation(station_key),
    FOREIGN KEY (end_station_key)
        REFERENCES DimStation(station_key),
    FOREIGN KEY (date_key)
        REFERENCES DimDate(date_key),
    FOREIGN KEY (weather_key)
        REFERENCES DimWeather(weather_key),
    FOREIGN KEY (incident_key)
        REFERENCES DimIncident(incident_key)
);

-- ── Verify ───────────────────────────────────────────────────
SELECT
    t.TABLE_NAME,
    COUNT(c.COLUMN_NAME) AS column_count
FROM INFORMATION_SCHEMA.TABLES t
JOIN INFORMATION_SCHEMA.COLUMNS c
    ON t.TABLE_NAME = c.TABLE_NAME
WHERE t.TABLE_TYPE = 'BASE TABLE'
GROUP BY t.TABLE_NAME
ORDER BY t.TABLE_NAME;
