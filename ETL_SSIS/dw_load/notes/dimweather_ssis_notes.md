# DimWeather — SSIS ETL Notes

## Overview
Loads weather data from TWO staging sources into DimWeather
dimension table using Union All to merge both streams.

**Source 1:** BerlinSBahn_Staging → dbo.stg_weather_openmeteo  
**Source 2:** BerlinSBahn_Staging → dbo.stg_weather_kaggle  
**Destination:** BerlinSBahn_DW → dbo.DimWeather  
**SCD Type:** Type 1 (upsert via stored procedure)  
**Rows Loaded:** 8784 unique weather records  

---

## Data Flow Components

```
[Extract from stg_weather_openmeteo]    [Extract from stg_weather_kaggle]
             ↓                                        ↓
   [Tag OpenMeteo Source]                   [Tag Kaggle Source]
   (Derived Column)                         (Derived Column)
             ↓                                        ↓
                                    [Convert Kaggle Timestamp]
                                    (Data Conversion)
             ↓                                        ↓
     [Sort OpenMeteo]                        [Sort Kaggle]
     (sort by timestamp)                     (sort by Copy of timestamp)
             ↓                                        ↓
             └──────→ [Combine Weather Sources] ←─────┘
                       (Union All)
                              ↓
                      [Load DimWeather]
                      (OLE DB Command)
```

---

## Step 1: Extract Sources (OLE DB Source x2)

### Source 1 — stg_weather_openmeteo
- Connection: BerlinSBahn_Staging
- Table: dbo.stg_weather_openmeteo
- Columns: timestamp (datetime), temperature_c, precipitation_mm,
           wind_speed_kmh, weather_condition

### Source 2 — stg_weather_kaggle
- Connection: BerlinSBahn_Staging
- Table: dbo.stg_weather_kaggle
- Columns: timestamp (varchar), temperature_c, precipitation_mm,
           wind_speed_kmh, weather_condition

---

## Step 2: Tag OpenMeteo Source (Derived Column)

| New Column | Expression | Purpose |
|---|---|---|
| `data_source` | `"Open-Meteo"` | Tag source |
| `temp_category` | `temperature_c < 5 ? "Cold" : temperature_c < 15 ? "Mild" : temperature_c < 25 ? "Warm" : "Hot"` | Categorize temperature |

---

## Step 3: Tag Kaggle Source (Derived Column)

| New Column | Expression | Purpose |
|---|---|---|
| `data_source` | `"Kaggle"` | Tag source |
| `temp_category` | `temperature_c < 5 ? "Cold" : temperature_c < 15 ? "Mild" : temperature_c < 25 ? "Warm" : "Hot"` | Categorize temperature |

---

## Step 4: Convert Kaggle Timestamp (Data Conversion)
- Kaggle timestamp is VARCHAR — must convert to DATETIME
  before Union All
- Input: `timestamp` (string)
- Output: `Copy of timestamp` (database timestamp DT_DBTIMESTAMP)

---

## Step 5: Sort Components
Both streams sorted by timestamp before Union All

---

## Step 6: Combine Weather Sources (Union All)

| Output Column | Input 1 (OpenMeteo) | Input 2 (Kaggle) |
|---|---|---|
| timestamp | timestamp | Copy of timestamp |
| temperature_c | temperature_c | temperature_c |
| precipitation_mm | precipitation_mm | precipitation_mm |
| wind_speed_kmh | wind_speed_kmh | wind_speed_kmh |
| weather_condition | weather_condition | weather_condition |
| data_source | data_source | data_source |
| temp_category | temp_category | temp_category |

---

## Step 7: Load DimWeather (OLE DB Command)
- Connection: BerlinSBahn_DW
- SQL: `exec dbo.UpsertDimWeather ?, ?, ?, ?, ?, ?`

| Parameter | Input Column |
|---|---|
| Param_0 (@timestamp) | timestamp |
| Param_1 (@temperature_c) | temperature_c |
| Param_2 (@precipitation_mm) | precipitation_mm |
| Param_3 (@wind_speed_kmh) | wind_speed_kmh |
| Param_4 (@weather_condition) | weather_condition |
| Param_5 (@data_source) | data_source |

---

## Stored Procedure Logic
UpsertDimWeather checks if timestamp exists:
- If NOT exists → INSERT new record with temp_category derived
- If exists → UPDATE existing record

Derived inside stored procedure:
- temp_category: Cold(<5°C) / Mild(5-15°C) / Warm(15-25°C) / Hot(>25°C)

---

## Key Design Decisions

### Why Union All instead of Merge Join?
Both sources have identical column structures and we want ALL
rows from both sources combined. Union All stacks rows together
without any join condition — correct for same-structure sources.

### Why Data Conversion for Kaggle timestamp?
stg_weather_openmeteo stores timestamp as DATETIME but
stg_weather_kaggle stores it as VARCHAR. Union All requires
matching data types so Data Conversion component converts
Kaggle VARCHAR → DATETIME before merging.

### Why Upsert instead of Insert?
Both sources share overlapping timestamps (same time periods).
The stored procedure prevents duplicate timestamps by updating
existing records rather than inserting duplicates.

---

## Verification Results

| data_source | row_count |
|---|---|
| Open-Meteo | 23 |
| Kaggle | 8761 |
| **Total** | **8784** |

| temp_category | row_count |
|---|---|
| Cold | 233 |
| Mild | 4176 |
| Warm | 4157 |
| Hot | 218 |

---

## Type Mismatch Issues Resolved
- `weather_condition`: procedure uses VARCHAR(50) to match staging varchar
- `data_source`: procedure uses NVARCHAR(30) to match SSIS unicode DT_WSTR
- `timestamp`: Kaggle varchar converted via Data Conversion component
