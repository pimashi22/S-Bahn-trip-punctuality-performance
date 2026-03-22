# DimIncident — SSIS ETL Notes

## Overview
Loads S-Bahn incident data from staging into DimIncident
dimension table using a Lookup to resolve line surrogate key.

**Source:** BerlinSBahn_Staging → dbo.stg_incidents  
**Destination:** BerlinSBahn_DW → dbo.DimIncident  
**SCD Type:** Type 1 (upsert via stored procedure)  
**Rows Loaded:** 36  

---

## Data Flow Components

```
[Extract from stg_incidents]
           ↓
    [Line Key Lookup]
    (Lookup Match Output)
           ↓
    [Load DimIncident]
    (OLE DB Command)
```

---

## Step 1: Extract from stg_incidents (OLE DB Source)
- Connection: BerlinSBahn_Staging
- Table: dbo.stg_incidents
- Columns: incident_id, timestamp, incident_type,
           delay_impact_factor, line_id

---

## Step 2: Line Key Lookup (Lookup)
Replaces `line_id` (varchar) with `line_key` (int surrogate key)
from DimLine — same technique as Lab Sheet 04 Section 5.

### Settings
- Cache mode: Full cache
- Connection: BerlinSBahn_DW
- Lookup table: dbo.DimLine
- No match handling: Ignore failure

### Column Mapping
| Input Column | Lookup Column | Output Alias |
|---|---|---|
| line_id | line_id (join) | — |
| — | line_key (retrieve) | line_key |

---

## Step 3: Load DimIncident (OLE DB Command)
- Connection: BerlinSBahn_DW
- SQL: `exec dbo.UpsertDimIncident ?, ?, ?, ?, ?`

| Parameter | Input Column |
|---|---|
| Param_0 (@incident_id) | incident_id |
| Param_1 (@incident_type) | incident_type |
| Param_2 (@delay_impact_factor) | delay_impact_factor |
| Param_3 (@line_key) | line_key |
| Param_4 (@incident_timestamp) | timestamp |

---

## Stored Procedure Logic
UpsertDimIncident checks if incident_id exists:
- If NOT exists → INSERT new record
- If exists → UPDATE existing record

Transformations inside stored procedure:
- `incident_date`: extracted from timestamp (DATE part only)
- `incident_hour`: extracted from timestamp (HOUR part)
- `severity_category`: Low(≤2) / Medium(≤5) / High(>5)
  based on delay_impact_factor

---

## Verification Results

### Row Count
| Metric | Value |
|---|---|
| Total rows | 36 |

### Severity Distribution
| severity_category | row_count |
|---|---|
| High | 9 |
| Low | 11 |
| Medium | 16 |

---

## Design Notes

### Why Lookup instead of Merge Join?
Lookup is simpler and more efficient for retrieving a single
surrogate key from a dimension table. Lab Sheet 04 Section 5
demonstrates this is the preferred approach for key resolution.

### Why incident_key is NULL in FactTrip
stg_trips has no incident_id column — there is no direct
trip-to-incident relationship in the source data. incident_key
is nullable in FactTrip by design. DimIncident is linked to
DimLine via line_key, enabling incident analysis by line,
severity, and time independently of the fact table.

### Relationship to FactTrip
Although incident_key is NULL in FactTrip, DimIncident
still supports rich analysis:
- Incidents per line
- Incidents by severity
- Incidents by time of day
- Incidents by date/month
