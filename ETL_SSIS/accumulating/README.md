# Task 06: ETL – Accumulating Fact Tables
## Package: `BerlinSBahn_Accumulating_Fact_Update.dtsx`

---

## Purpose
This SSIS package reads trip completion data from a CSV file and updates 
the `FactTrip` table in `BerlinSBahn_DW` with:
- `accm_txn_complete_time` — when the trip transaction was completed
- `txn_process_time_hours` — hours between creation and completion

---

## Package Location
```
BerlinSBahn_ETL_IT23727854/
└── SSIS Packages/
    └── BerlinSBahn_Accumulating_Fact_Update.dtsx
```

---

## Control Flow
```
┌─────────────────────────────────────┐
│  Update FactTrip Completion Times   │
│         (Data Flow Task)            │
└─────────────────────────────────────┘
```
Single Data Flow Task that reads CSV and updates FactTrip rows.

---

## Data Flow
```
┌──────────────────────────┐
│  Read Trip Completion CSV │  ← Flat File Source
│  C:\BerlinSBahn\          │    trip_completion_data.csv
│  trip_completion_data.csv │
└────────────┬─────────────┘
             ↓
┌──────────────────────────┐
│     Derived Column        │  ← Creates complete_time_copy
│                           │    (DT_DBTIMESTAMP cast of
│                           │     accm_txn_complete_time)
└────────────┬─────────────┘
             ↓
┌──────────────────────────┐
│     Data Conversion       │  ← Converts accm_txn_complete_time
│                           │    string → DT_DBTIMESTAMP
│                           │    Output: Copy of accm_txn_complete_time
└────────────┬─────────────┘
             ↓
┌──────────────────────────┐
│    Script Component       │  ← Executes UPDATE on FactTrip
│    (Transformation)       │    per row using SqlConnection
└──────────────────────────┘
```

---

## Connection Managers
| Name | Type | Points To |
|---|---|---|
| Flat File Connection Manager | Flat File | `C:\BerlinSBahn\trip_completion_data.csv` |
| BerlinSBahn_DW | OLE DB | `BerlinSBahn_DW` SQL Server database |

---

## Files in This Folder
| File | Description |
|---|---|
| `README.md` | This file — package overview |
| `01_extend_facttable.sql` | SQL to add accumulating columns to FactTrip |
| `02_trip_completion_data.csv` | Source CSV with txn_id + completion times |
| `03_script_component_code.cs` | C# code used inside the Script Component |
| `04_verify_accumulating.sql` | Verification queries to confirm correct updates |
| `screenshots/` | Screenshots of package execution |
