# Task 3 — Solution Architecture

## Architecture Overview

The solution follows a classic **three-tier Data Warehouse architecture** consisting of a Source Layer, a Staging Layer, and a Data Warehouse Layer, with a BI/Reporting layer on top. The ETL process is implemented using **SSIS (SQL Server Integration Services)** across three separate packages.

---

## Architecture Diagram

```
╔══════════════════════════════════════════════════════════════════╗
║                         SOURCE LAYER                            ║
║                                                                  ║
║  ┌─────────────────────┐      ┌──────────────────────────────┐  ║
║  │   Source Type 1     │      │       Source Type 2          │  ║
║  │   Flat Files (CSV)  │      │   SQL Server Database        │  ║
║  │                     │      │   BerlinSBahn_Source         │  ║
║  │  • trips.csv        │      │                              │  ║
║  │  • lines.csv        │      │  stg_weather_openmeteo       │  ║
║  │  • stations.csv     │      │  (Open-Meteo Historical      │  ║
║  │  • incidents.csv    │      │   Weather API — Berlin       │  ║
║  │  • weather.csv      │      │   2024 — 8,784 rows)         │  ║
║  └─────────────────────┘      └──────────────────────────────┘  ║
╚══════════════════════╤═══════════════════╤═══════════════════════╝
                       │                   │
                       └─────────┬─────────┘
                                 │
                    ┌────────────▼────────────┐
                    │    SSIS Package 1        │
                    │  BerlinSBahn_Load_       │
                    │    Staging.dtsx          │
                    │                          │
                    │  6 Data Flow Tasks:      │
                    │  • Extract Lines         │
                    │  • Extract Stations      │
                    │  • Extract Incidents     │
                    │  • Extract Kaggle        │
                    │    Weather               │
                    │  • Extract Trips         │
                    │  • Extract OpenMeteo     │
                    │    Weather (SQL Source)  │
                    └────────────┬────────────┘
                                 │
╔════════════════════════════════▼═════════════════════════════════╗
║                        STAGING LAYER                            ║
║                      BerlinSBahn_Staging                        ║
║                                                                  ║
║  stg_lines          stg_stations        stg_incidents           ║
║  stg_weather_kaggle stg_weather_openmeteo stg_trips             ║
║                                                                  ║
║  Purpose: Raw data holding area before transformation.          ║
║  No foreign keys. Mirrors source structure exactly.             ║
║  Truncated before each load via OnPreExecute Event Handler.     ║
╚════════════════════════════════╤═════════════════════════════════╝
                                 │
                    ┌────────────▼────────────┐
                    │    SSIS Package 2        │
                    │  BerlinSBahn_Load_       │
                    │      DW.dtsx             │
                    │                          │
                    │  Transform + Load:       │
                    │  • DimLine               │
                    │    (Stored Procedure)    │
                    │  • DimStation            │
                    │    (SCD Type 2 Wizard)   │
                    │  • DimWeather            │
                    │    (Union All merge)     │
                    │  • DimIncident           │
                    │    (Lookup transform)    │
                    │  • FactTrip              │
                    │    (5 Lookups)           │
                    └────────────┬────────────┘
                                 │
╔════════════════════════════════▼═════════════════════════════════╗
║                     DATA WAREHOUSE LAYER                        ║
║                       BerlinSBahn_DW                            ║
║                                                                  ║
║  ┌──────────┐  ┌──────────┐  ┌────────────┐  ┌─────────────┐  ║
║  │ DimDate  │  │ DimLine  │  │ DimStation │  │ DimWeather  │  ║
║  │ 366 rows │  │  6 rows  │  │  10 rows   │  │ 17,545 rows │  ║
║  │ (static) │  │ (Type 1) │  │  (Type 2)  │  │  (Type 1)   │  ║
║  └────┬─────┘  └────┬─────┘  └─────┬──────┘  └──────┬──────┘  ║
║       │              │              │                  │         ║
║       └──────────────┴──────────────┴──────────────────┘        ║
║                              │                                   ║
║                    ┌─────────▼──────────┐                       ║
║                    │     FactTrip        │  ┌─────────────┐     ║
║                    │   131,771 rows      ├──┤ DimIncident │     ║
║                    │  (Accumulating)     │  │  36 rows    │     ║
║                    └─────────────────────┘  └─────────────┘     ║
╚════════════════════════════════╤═════════════════════════════════╝
                                 │
                    ┌────────────▼────────────┐
                    │    SSIS Package 3        │
                    │  BerlinSBahn_Update_     │
                    │   Completion.dtsx        │
                    │                          │
                    │  Updates FactTrip:       │
                    │  • accm_txn_complete_time│
                    │  • txn_process_time_hours│
                    └────────────┬────────────┘
                                 │
╔════════════════════════════════▼═════════════════════════════════╗
║                      BI / REPORTING LAYER                       ║
║                                                                  ║
║  ┌────────────────┐  ┌─────────────────┐  ┌──────────────────┐ ║
║  │   SSAS Cube    │  │  SSRS Reports   │  │ Excel Analytics  │ ║
║  │                │  │                 │  │                  │ ║
║  │ Student Perf   │  │ Delay Analysis  │  │ Pivot Tables     │ ║
║  │ Analytics Cube │  │ Line Reports    │  │ Charts           │ ║
║  └────────────────┘  └─────────────────┘  └──────────────────┘ ║
╚══════════════════════════════════════════════════════════════════╝
```

---

## Component Descriptions

### Source Layer

**Source Type 1 — Flat Files (CSV)**
Five CSV files downloaded from Kaggle containing Berlin S-Bahn operational data for the year 2024. These files are stored locally at `C:\BerlinSBahn\` and read by SSIS using Flat File Source components. This source type represents structured operational data exports.

**Source Type 2 — SQL Server Database (BerlinSBahn_Source)**
Real hourly meteorological data for Berlin fetched from the Open-Meteo Historical Weather API using a Python script. The data covers the same period as the operational data (Jan–Dec 2024) at hourly granularity. The fetched data was loaded into a dedicated SQL Server database (`BerlinSBahn_Source`) to serve as a proper relational data source, demonstrating integration of a database source type alongside flat files.

---

### Staging Layer (BerlinSBahn_Staging)

The staging database serves as a temporary holding area between source systems and the data warehouse. Its purpose is to:

- Decouple source extraction from transformation logic
- Allow data profiling and quality checks before loading the DW
- Provide a recovery point if DW loads fail
- Store data in its raw form without any transformation applied

All staging tables mirror their source structure exactly. BIT columns such as `is_ring_line` and `is_major_hub` are stored as `VARCHAR(10)` in staging (holding "True"/"False" text) and converted to proper BIT values during the DW load phase.

Each staging table is truncated before every load using an `OnPreExecute` Event Handler on each Data Flow Task, ensuring no stale data accumulates.

---

### ETL Layer (SSIS)

Three SSIS packages implement the full ETL pipeline:

**Package 1 — BerlinSBahn_Load_Staging.dtsx**
Responsible for extraction only. Reads from both source types and populates all six staging tables. No transformation is applied at this stage. Tasks run serially to manage resource usage.

**Package 2 — BerlinSBahn_Load_DW.dtsx**
Responsible for transformation and loading. Reads from staging, applies all business transformations, resolves surrogate keys via Lookup components, and loads each dimension and the fact table in FK dependency order. Key transformations include:

- Boolean text conversion (True/False → BIT)
- Derived category columns (delay_category, temp_category, time_of_day, line_category, severity_category)
- Surrogate key resolution via Lookup transformations
- SCD Type 2 implementation for DimStation
- Union All merge of two weather sources
- Stored procedure upsert logic for Type 1 dimensions

**Package 3 — BerlinSBahn_Update_Completion.dtsx**
Responsible for updating the accumulating fact table. Reads a separate completion dataset and updates `accm_txn_complete_time` and `txn_process_time_hours` in FactTrip using an OLE DB Command with parameterised UPDATE statements.

---

### Data Warehouse Layer (BerlinSBahn_DW)

The data warehouse implements a **Star Schema** with five dimension tables and one central fact table. The schema was designed following Kimball dimensional modelling principles:

- All dimensions carry surrogate keys (IDENTITY columns)
- Natural/alternate keys are preserved for source system traceability
- `insert_date` and `modified_date` audit columns on all tables
- DimStation implements SCD Type 2 with `effective_start_date`, `effective_end_date`, and `is_current` columns
- FactTrip implements the Accumulating Snapshot pattern with create/complete timestamps

---

### BI and Reporting Layer

**SSAS (SQL Server Analysis Services)**
An OLAP cube named "S-Bahn Punctuality Analytics" is built on top of the star schema, enabling multidimensional analysis with hierarchies for Date (Day → Month → Quarter → Year), Location (Station → Line), and Delay Category.

**SSRS (SQL Server Reporting Services)**
Tabular and graphical reports are created to visualise key performance indicators including delay rates by line, punctuality trends by month, weather impact analysis, and station performance comparisons.

---

## Technology Stack Summary

| Component | Technology |
|-----------|-----------|
| Source Database | SQL Server 2017+ |
| Staging Database | SQL Server 2017+ |
| Data Warehouse | SQL Server 2017+ |
| ETL Tool | SSIS (Visual Studio 2022 SSDT) |
| OLAP Cube | SSAS |
| Reporting | SSRS |
| API Data Fetch | Python 3.x (requests, pandas) |
| Version Control | GitHub |

---

## ETL Execution Order

```
Step 1: Run DW_Design/01_create_all_tables.sql
Step 2: Run DW_Design/02_populate_dimdate.sql
Step 3: Run all stored procedures in ETL_SSIS/dw_load/stored_procedures/
Step 4: Execute BerlinSBahn_Load_Staging.dtsx
Step 5: Execute BerlinSBahn_Load_DW.dtsx
Step 6: Execute BerlinSBahn_Update_Completion.dtsx
```

DW load order within Package 2 (FK dependency order):
```
DimDate (pre-loaded) → DimLine → DimStation → 
DimWeather → DimIncident → FactTrip
```
