# 05 — FactTrip SSIS Load Notes

## Overview
Loads 131,771 rows from `BerlinSBahn_Staging.dbo.stg_trips` into  
`BerlinSBahn_DW.dbo.FactTrip` via a multi-step SSIS Data Flow.

---

## Pre-requisites (run before SSIS execution)

### 1. Create the staging view (BerlinSBahn_Staging)
Run: `05_prepare_facttrip_view.sql`  
Creates `dbo.vw_stg_trips_prepared` which pre-computes:
- Explicit `INT` cast on `trip_id`
- `varchar 'True'/'False'` → `BIT` for `is_delayed`, `is_cancelled`, `is_peak_hour`
- `date_key_val` (INT YYYYMMDD) for DimDate lookup
- `weather_timestamp` (truncated to hour) for DimWeather lookup
- `delay_category`, `time_of_day`, `performance_score` derived columns

### 2. Fix FactTrip schema (BerlinSBahn_DW)
Run: `05_fix_facttrip_schema.sql`  
Makes these one-time changes:
| Fix | Reason |
|-----|--------|
| `weather_key` → NULL | Some trips have no matching weather record |
| `trip_duration_cat` → NULL | Not populated by this pipeline |
| Disable `incident_key` FK | All trips load with NULL incident_key |

---

## SSIS Data Flow Layout

```
Extract from stg_trips  (OLE DB Source → vw_stg_trips_prepared)
         ↓
  Line Key Lookup        (DimLine: line_id → line_key)
         ↓
Start Station Lookup     (DimStation: start_station_id → start_station_key)
         ↓
 End Station Lookup      (DimStation: end_station_id → end_station_key)
         ↓
  Date Key Lookup        (DimDate: date_key_val → date_key)
         ↓
Weather Key Lookup       (DimWeather: weather_timestamp → weather_key)
         ↓
  Add Audit Columns      (Derived Column: insert_date, modified_date, accm_txn_create_time)
         ↓
     Load FactTrip       (OLE DB Destination → dbo.FactTrip, fast load)
```

---

## Lookup Configuration (all 5 lookups)

| Setting | Value |
|---------|-------|
| Cache mode | Full cache |
| Connection type | OLE DB |
| No match behaviour | Ignore failure |
| Error output | Ignore failure |

---

## OLE DB Destination Mappings

| Input Column | Destination Column |
|---|---|
| trip_id | trip_id |
| line_key | line_key |
| start_station_key | start_station_key |
| end_station_key | end_station_key |
| date_key | date_key |
| weather_key | weather_key |
| `<ignore>` | incident_key |
| scheduled_departure | scheduled_departure |
| delay_minutes | delay_minutes |
| is_delayed | is_delayed |
| is_cancelled | is_cancelled |
| is_peak_hour | is_peak_hour |
| delay_category | delay_category |
| time_of_day | time_of_day |
| performance_score | performance_score |
| accm_txn_create_time | accm_txn_create_time |
| insert_date | insert_date |
| modified_date | modified_date |
| `<ignore>` | trip_key |
| `<ignore>` | trip_duration_cat |
| `<ignore>` | accm_txn_complete_time |
| `<ignore>` | txn_process_time_hours |

---

## Errors Encountered & Resolutions

| Error | Root Cause | Fix |
|-------|-----------|-----|
| `DTS_E_OLEDBERROR 0x80004005` on `is_delayed` | SSIS cached old varchar metadata after view was updated to BIT | Opened OLE DB Source editor → clicked **Yes** to refresh metadata |
| `DTS_E_OLEDBERROR 0x80004005` — 9,878 rows then fail | `trip_duration_cat` column was NOT NULL but not mapped | `ALTER COLUMN trip_duration_cat VARCHAR(50) NULL` |
| FK violation on `incident_key` | FK constraint blocked NULL inserts | `NOCHECK CONSTRAINT FK__FactTrip__incide__01142BA1` |
| `weather_key` NULL violation | Some trips had no weather match | `ALTER COLUMN weather_key INT NULL` |

---

## Post-load Verification

Run: `05_verify_facttrip.sql`

### Expected Results
| Check | Expected |
|-------|---------|
| Total rows | 131,771 |
| null_line_keys | 0 |
| null_start_stations | 0 |
| null_end_stations | 0 |
| null_date_keys | 0 |
| null_weather_keys | 0 |
| null_incident_keys | 131,771 (all NULL — expected) |
