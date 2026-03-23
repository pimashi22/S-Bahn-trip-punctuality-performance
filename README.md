# Berlin S-Bahn Punctuality Analytics
### An End-to-End Data Warehouse & Business Intelligence Solution

![SQL Server](https://img.shields.io/badge/SQL%20Server-CC2927?style=flat&logo=microsoft-sql-server&logoColor=white)
![SSIS](https://img.shields.io/badge/SSIS-0078D4?style=flat&logo=microsoft&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)
![GitHub](https://img.shields.io/badge/GitHub-181717?style=flat&logo=github&logoColor=white)

---

## 📌 Overview

This project delivers a production-grade **Data Warehouse and Business Intelligence solution** built on Berlin S-Bahn (suburban railway) operational data spanning the full calendar year of 2024. It demonstrates the complete DW/BI engineering lifecycle — from raw multi-source data ingestion and staging, through ETL transformation pipelines, to a fully normalised star schema data warehouse ready for OLAP analysis and reporting.

The solution integrates **two distinct data source types** — CSV flat files from Kaggle and a real-time meteorological SQL Server database sourced from the Open-Meteo Historical Weather API — and processes over **131,000 trip records** alongside **17,500 weather observations** to produce a unified analytical platform for S-Bahn punctuality performance.

> **Why this matters:** Urban transit punctuality directly impacts the quality of life of millions of commuters. This solution enables transport analysts and operations teams to identify delay patterns, correlate disruptions with weather and infrastructure incidents, and make data-driven decisions to improve service reliability.

---

## ✨ Key Highlights

- **131,771** S-Bahn trips analysed across 6 lines and 10 stations
- **Two source types** — CSV flat files and a SQL Server relational database
- **Three-stage architecture** — Source → Staging → Data Warehouse
- **Three SSIS ETL packages** covering extraction, transformation, loading, and fact updates
- **SCD Type 2** implemented on the Station dimension to track location changes over time
- **Accumulating Snapshot Fact Table** tracking transaction lifecycle from creation to completion
- **Stored procedure upsert logic** ensuring idempotent, re-runnable ETL loads
- **Union All merge** of two independent weather data sources with lineage tracking

---

## 📋 Table of Contents

- [Architecture](#architecture)
- [Data Sources](#data-sources)
- [Data Warehouse Design](#data-warehouse-design)
- [ETL Pipeline](#etl-pipeline)
- [Repository Structure](#repository-structure)
- [Getting Started](#getting-started)
- [Key Metrics](#key-metrics)
- [Technologies](#technologies)

---

## 🏗️ Architecture

The solution follows the industry-standard **Kimball three-tier Data Warehouse architecture**:

```
┌─────────────────────────────────────────────────────────────┐
│                       SOURCE LAYER                          │
│                                                             │
│   ┌──────────────────┐        ┌────────────────────────┐   │
│   │  CSV Flat Files  │        │  SQL Server Database   │   │
│   │  (Kaggle)        │        │  (Open-Meteo API)      │   │
│   │                  │        │  BerlinSBahn_Source    │   │
│   │  trips.csv       │        │  8,784 hourly rows     │   │
│   │  lines.csv       │        │  Berlin 2024           │   │
│   │  stations.csv    │        └────────────────────────┘   │
│   │  incidents.csv   │                                     │
│   │  weather.csv     │                                     │
│   └──────────────────┘                                     │
└──────────────────────┬──────────────────────────────────────┘
                       │  Package 1 — BerlinSBahn_Load_Staging.dtsx
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                      STAGING LAYER                          │
│                   BerlinSBahn_Staging                       │
│                                                             │
│  stg_lines · stg_stations · stg_incidents · stg_trips      │
│  stg_weather_kaggle · stg_weather_openmeteo                 │
└──────────────────────┬──────────────────────────────────────┘
                       │  Package 2 — BerlinSBahn_Load_DW.dtsx
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                   DATA WAREHOUSE LAYER                      │
│                     BerlinSBahn_DW                          │
│                                                             │
│   DimDate · DimLine · DimStation · DimWeather               │
│   DimIncident · FactTrip                                    │
└──────────────────────┬──────────────────────────────────────┘
                       │  Package 3 — BerlinSBahn_Update_Completion.dtsx
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                    BI / REPORTING LAYER                     │
│                                                             │
│        SSAS Cube · SSRS Reports · Excel Analytics          │
└─────────────────────────────────────────────────────────────┘
```

Full architecture documentation → [`Architecture/architecture_notes.md`](Architecture/architecture_notes.md)

---

## 🗄️ Data Sources

### Source 1 — Kaggle CSV Dataset

Five structured CSV files representing Berlin S-Bahn operational records for 2024, loaded using SSIS Flat File Source components.

| File | Rows | Contents |
|------|------|----------|
| `trips.csv` | 131,771 | Trip records — delay, cancellation, departure data |
| `lines.csv` | 6 | Line reference — S1, S2, S5, S7, S41, S42 |
| `stations.csv` | 10 | Station reference — name, hub status, location type |
| `incidents.csv` | 36 | Infrastructure incidents — type, impact factor, affected line |
| `weather.csv` | 8,761 | Hourly weather — temperature, precipitation, wind, condition |

### Source 2 — Open-Meteo Historical Weather API

Real hourly meteorological data for Berlin fetched via the [Open-Meteo Historical Weather API](https://open-meteo.com/) using a Python script. Pre-loaded into a dedicated SQL Server database to demonstrate relational database source integration alongside flat files.

```
Location    : Berlin, Germany (52.52°N, 13.41°E)
Period      : 2024-01-01 to 2024-12-31
Granularity : Hourly — 8,784 rows
Variables   : temperature_2m, precipitation, wind_speed_10m, weather_code
```

Fetch script → [`DataSources/source2_openmeteo/fetch_weather.py`](DataSources/source2_openmeteo/fetch_weather.py)

---

## ⭐ Data Warehouse Design

### Star Schema

A **Star Schema** with five dimension tables surrounding one central accumulating fact table.

```
                       ┌────────────┐
                       │  DimDate   │
                       │  366 rows  │
                       └─────┬──────┘
                             │
  ┌──────────┐    ┌──────────┴──────────┐    ┌────────────┐
  │ DimLine  │    │      FactTrip        │    │ DimWeather │
  │  6 rows  ├────┤    131,771 rows      ├────┤ 17,545 rows│
  └──────────┘    │   (Accumulating)     │    └────────────┘
                  └──────┬───────┬───────┘
                         │       │
              ┌──────────┘       └──────────┐
       ┌──────┴──────┐              ┌───────┴──────┐
       │ DimStation  │              │ DimIncident  │
       │  10+ rows   │              │   36 rows    │
       │ (SCD Type 2)│              └──────────────┘
       │Role-playing │
       └─────────────┘
```

### Dimension Summary

| Dimension | SCD Type | Rows | Key Design Decision |
|-----------|----------|------|---------------------|
| DimDate | Static | 366 | Berlin public holidays, seasons, weekend flags |
| DimLine | Type 1 | 6 | Ring vs Suburban categorisation, delay propensity |
| DimStation | **Type 2** | 10+ | Tracks location_category changes over time with effective dates |
| DimWeather | Type 1 | 17,545 | Merged from two sources; temp_category derived |
| DimIncident | Type 1 | 36 | Severity derived from delay_impact_factor |

### Fact Table Design

`FactTrip` implements the **Accumulating Snapshot** pattern:

| Column Group | Columns |
|-------------|---------|
| Dimension FKs | line_key, start_station_key, end_station_key, date_key, weather_key, incident_key |
| Measures | delay_minutes, performance_score |
| Flags | is_delayed, is_cancelled, is_peak_hour |
| Derived | delay_category, time_of_day, trip_duration_cat |
| Accumulating | accm_txn_create_time, accm_txn_complete_time, txn_process_time_hours |

Full schema → [`DW_Design/create_all_tables.sql`](DW_Design/create_all_tables.sql)

---

## ⚙️ ETL Pipeline

### Package 1 — Extract to Staging

Six Data Flow Tasks run serially. Each includes an `OnPreExecute` Event Handler that truncates the staging table before loading to ensure clean, idempotent runs.

| Task | Source Type | Rows Loaded |
|------|------------|-------------|
| Extract Lines Data to Staging | Flat File (CSV) | 6 |
| Extract Stations Data to Staging | Flat File (CSV) | 10 |
| Extract Incidents Data to Staging | Flat File (CSV) | 36 |
| Extract Kaggle Weather Data to Staging | Flat File (CSV) | 8,761 |
| Extract Trips Data to Staging | Flat File (CSV) | 131,771 |
| Extract OpenMeteo Weather to Staging | **OLE DB (SQL Server)** | 8,784 |

### Package 2 — Transform and Load DW

Five Data Flow Tasks in strict FK dependency order, demonstrating a rich set of SSIS transformation components.

| Task | Key Components | Transformations Applied |
|------|---------------|------------------------|
| Load DimLine | Derived Column → OLE DB Command | True/False → BIT; line_category derivation; SP upsert |
| Load DimStation | Derived Column → **SCD Wizard** | SCD Type 2 with effective dates |
| Load DimWeather | 2× Source → Sort → **Union All** → OLE DB Command | Merge two weather sources; temp_category |
| Load DimIncident | **Lookup** → OLE DB Command | FK resolution; timestamp parsing; severity derivation |
| Load FactTrip | **5× Lookup** → Derived Column → OLE DB Destination | All surrogate key resolutions; all derived columns |

### Package 3 — Accumulating Fact Update

Reads a completion dataset and updates two columns in FactTrip:

```sql
UPDATE FactTrip
SET accm_txn_complete_time = ?,
    txn_process_time_hours = DATEDIFF(HOUR, accm_txn_create_time, ?)
WHERE trip_id = ?
```

---

## 📁 Repository Structure

```
S-Bahn-trip-punctuality-performance/
│
├── README.md                          ← Project overview (this file)
│
├── Dataset/
│   └── README.md                      ← Dataset description, ERD, statistics
│
├── DataSources/
│   ├── source1_kaggle/
│   │   ├── source1_info.md            ← Schema and file descriptions
│   │   ├── trips.csv
│   │   ├── lines.csv
│   │   ├── stations.csv
│   │   ├── incidents.csv
│   │   └── weather.csv
│   └── source2_openmeteo/
│       ├── fetch_weather.py           ← Open-Meteo API fetch script
│       ├── weather_openmeteo.csv      ← Fetched data (8,784 rows)
│       ├── create_source_db.sql       ← Create source staging table
│       └── bulk_insert_weather.sql    ← Load CSV into SQL Server
│
├── Architecture/
│   └── architecture_notes.md         ← Full architecture diagram + descriptions
│
├── DW_Design/
│   ├── create_all_tables.sql         ← CREATE TABLE for all 6 DW tables
│   └── populate_dimdate.sql          ← Generate 366 DimDate rows for 2024
│
├── ETL_SSIS/
│   ├── staging/
│   │   ├── staging_tables_info.md    ← Staging table descriptions
│   │   └── verify_staging.sql        ← Staging row count verification
│   ├── dw_load/
│   │   ├── stored_procedures/
│   │   │   ├── upsert_dimline.sql    ← SP for DimLine upsert
│   │   │   ├── upsert_dimweather.sql ← SP for DimWeather upsert
│   │   │   └── upsert_dimincident.sql← SP for DimIncident upsert
│   │   ├── verification/
│   │   │   ├── verify_dimline.sql
│   │   │   ├── verify_dimstation.sql
│   │   │   ├── verify_dimweather.sql
│   │   │   ├── verify_dimincident.sql
│   │   │   └── verify_facttrip.sql
│   │   └── notes/
│   │       ├── dimline_notes.md
│   │       ├── dimstation_scd_notes.md
│   │       ├── dimweather_notes.md
│   │       ├── dimincident_notes.md
│   │       └── facttrip_notes.md
│   └── accumulating/
│
└── Verification/
    ├── source_verify.sql             ← Verify source DB row counts
    ├── dw_verify.sql                 ← Verify DW table structure
    └── post_etl_verify.sql           ← Verify all tables after full ETL
```

---

## 🚀 Getting Started

### Prerequisites

```
- SQL Server 2017 or later
- SQL Server Management Studio (SSMS)
- Visual Studio 2022 with SQL Server Data Tools (SSDT)
- Python 3.x with requests and pandas libraries
```

### Step 1 — Clone the Repository

```bash
git clone https://github.com/pimashi22/S-Bahn-trip-punctuality-performance.git
cd S-Bahn-trip-punctuality-performance
```

### Step 2 — Create the Three Databases

```sql
CREATE DATABASE BerlinSBahn_Source;
CREATE DATABASE BerlinSBahn_Staging;
CREATE DATABASE BerlinSBahn_DW;
```

### Step 3 — Place CSV Files

Copy all files from `DataSources/source1_kaggle/` to `C:\BerlinSBahn\`

### Step 4 — Load Open-Meteo Source Data

```bash
# Option A — Use pre-fetched CSV (recommended)
# Run: DataSources/source2_openmeteo/create_source_db.sql
# Run: DataSources/source2_openmeteo/bulk_insert_weather.sql

# Option B — Re-fetch from API
cd DataSources/source2_openmeteo
python fetch_weather.py
```

### Step 5 — Build the DW Schema

In SSMS against `BerlinSBahn_DW`, run in order:
```
1. DW_Design/create_all_tables.sql
2. DW_Design/populate_dimdate.sql
```

### Step 6 — Create Stored Procedures

Run all files in `ETL_SSIS/dw_load/stored_procedures/` against `BerlinSBahn_DW`.

### Step 7 — Execute SSIS Packages

Open the solution in Visual Studio and run in order:
```
1. BerlinSBahn_Load_Staging.dtsx
2. BerlinSBahn_Load_DW.dtsx
3. BerlinSBahn_Update_Completion.dtsx
```

### Step 8 — Verify

```sql
-- Run Verification/post_etl_verify.sql
-- Expected results:
-- DimDate        366
-- DimLine          6
-- DimStation      10
-- DimWeather  17,545
-- DimIncident     36
-- FactTrip   131,771
```

---

## 📊 Key Metrics

| Metric | Value |
|--------|-------|
| Total trips analysed | 131,771 |
| Date coverage | Jan 1 — Dec 30, 2024 |
| S-Bahn lines | 6 |
| Stations | 10 |
| Infrastructure incidents | 36 |
| Weather records merged | 17,545 |
| On-time trips | 25,083 (19.0%) |
| Minor delays (1–5 min) | 101,950 (77.4%) |
| Moderate delays (6–15 min) | 1,848 (1.4%) |
| Severe delays (>15 min) | 2,890 (2.2%) |
| Cancelled trips | 860 (0.7%) |
| Max recorded delay | 1,181 minutes |

---

## 🛠️ Technologies

| Technology | Purpose |
|------------|---------|
| SQL Server 2017+ | Source, Staging, and Data Warehouse databases |
| SSMS | Database management and query execution |
| SSIS (Visual Studio 2022) | ETL package development |
| SSAS | OLAP cube development |
| SSRS | Report generation |
| Python 3.x | Open-Meteo API data fetch |
| GitHub | Version control and documentation |

---

## 📄 License

This project is open for educational and portfolio use.

Dataset credit: [alperenmyung on Kaggle](https://www.kaggle.com/datasets/alperenmyung/berlin-s-bahn-punctuality-database)  
Weather data: [Open-Meteo Historical Weather API](https://open-meteo.com/)

---

*Built with SQL Server · SSIS · Python · GitHub*
