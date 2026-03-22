# DimStation — SSIS ETL Notes

## Overview
Loads S-Bahn station data from staging into DimStation
dimension table using SCD Type 2 to preserve historical changes.

**Source:** BerlinSBahn_Staging → dbo.stg_stations  
**Destination:** BerlinSBahn_DW → dbo.DimStation  
**SCD Type:** Type 2 (historical attribute — new row on change)  
**Rows Loaded:** 10 (initial load)  

---

## Data Flow Components

```
[Extract from stg_stations]
          ↓
  [Convert Data Types]
  (Derived Column)
          ↓
    [Station SCD]
    (SCD Wizard)
       ↓       ↓
[Derived  ] [OLE DB    ]
[Column   ] [Command 1 ]
(expire old) (Type 1 update)
       ↓       ↓
[OLE DB  ]    ↓
[Command ]    ↓
       ↓       ↓
    [Union All]
          ↓
  [Derived Column 1]
          ↓
  [Insert Destination]
```

---

## Step 1: Extract from stg_stations (OLE DB Source)
- Connection: BerlinSBahn_Staging
- Table: dbo.stg_stations
- Columns: station_id, name, is_major_hub, location_category

---

## Step 2: Convert Data Types (Derived Column)
Transformations applied:

| New Column | Expression | Purpose |
|---|---|---|
| `station_name` | `name` | Rename column for DW |
| `is_major_hub_bit` | `is_major_hub == "True" ? (DT_BOOL)TRUE : (DT_BOOL)FALSE` | Convert varchar to BIT |
| `effective_start_date` | `GETDATE()` | Set SCD start date |

---

## Step 3: Station SCD (SCD Wizard)

### Page 1 — Table and Keys
| Input Column | Dimension Column | Key Type |
|---|---|---|
| station_id | station_id | Business key |
| station_name | station_name | Not a key column |
| is_major_hub_bit | is_major_hub | Not a key column |
| location_category | location_category | Not a key column |
| effective_start_date | effective_start_date | Not a key column |

### Page 2 — Change Types
| Dimension Column | Change Type |
|---|---|
| station_name | Historical attribute (Type 2) |
| is_major_hub | Changing attribute (Type 1) |
| location_category | Historical attribute (Type 2) |

### Page 4 — Historical Attribute Options
- Option: Use start and end dates
- Start date column: effective_start_date
- End date column: effective_end_date
- Variable: System::StartTime

### Page 5 — Inferred Members
- Enable inferred member support: UNCHECKED

---

## SCD Type 2 Behavior

When `station_name` or `location_category` changes:
1. Old record: `effective_end_date` = change date, `is_current` = 0
2. New record: inserted with new values, `effective_end_date` = NULL, `is_current` = 1

When `is_major_hub` changes:
- Existing record updated in place (Type 1 — no history kept)

---

## OLE DB Command — Expire Old Records
The left branch OLE DB Command uses this SQL to set is_current = 0:
```sql
UPDATE DimStation
SET effective_end_date = ?,
    is_current = 0
WHERE station_id = ? AND effective_end_date IS NULL
```

---

## Verification Results

### Initial Load
| Metric | Value |
|---|---|
| Total rows | 10 |
| is_current = 1 | 10 |
| effective_end_date = NULL | 10 |

### After SCD Type 2 Test (S6 location_category changed)
| station_key | location_category | effective_end_date | is_current |
|---|---|---|---|
| 6 | airport | 2026-03-21 | 0 |
| 11 | suburb | NULL | 1 |
| Total rows | 11 | | |

---

## Design Notes
- `effective_start_date` and `effective_end_date` changed from DATE to DATETIME
  to match SSIS DT_DBTIMESTAMP type
- Default constraint on `effective_start_date` was dropped before ALTER TABLE
- `station_name` column changed from VARCHAR to NVARCHAR to match SSIS unicode
