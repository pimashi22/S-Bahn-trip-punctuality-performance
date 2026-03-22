# DimLine — SSIS ETL Notes

## Overview
Loads S-Bahn line data from staging into DimLine 
dimension table in BerlinSBahn_DW.

**Source:** BerlinSBahn_Staging → dbo.stg_lines  
**Destination:** BerlinSBahn_DW → dbo.DimLine  
**SCD Type:** Type 1 (overwrite on change)  
**Rows Loaded:** 6  

---

## Data Flow Components
```
[Extract from stg_lines]
         ↓
[Add Derived Columns]
         ↓
[Load DimLine]
```

### 1. Extract from stg_lines (OLE DB Source)
- Connection: BerlinSBahn_Staging
- Table: dbo.stg_lines
- Columns: line_id, line_name, is_ring_line, 
           delay_propensity

### 2. Add Derived Columns (Derived Column)
Transformations applied:

| New Column | Expression | Purpose |
|------------|------------|---------|
| line_category | is_ring_line=="True"?"Ring":"Suburban" | Categorize line type |

### 3. Load DimLine (OLE DB Command)
- Connection: BerlinSBahn_DW
- Calls: exec dbo.UpsertDimLine ?, ?, ?, ?
- Parameters:
  - Param_0 → line_id
  - Param_1 → line_name
  - Param_2 → is_ring_line
  - Param_3 → delay_propensity

---

## Stored Procedure Logic
UpsertDimLine checks if line_id exists:
- If NOT exists → INSERT new record
- If exists → UPDATE existing record

Transformations inside stored procedure:
- is_ring_line (True/False text) → BIT (1/0)
- line_category derived from is_ring_line value

---

## Verification Results
| line_id | line_category | is_ring_line |
|---------|--------------|--------------|
| S1 | Suburban | 0 |
| S2 | Suburban | 0 |
| S5 | Suburban | 0 |
| S7 | Suburban | 0 |
| S41 | Ring | 1 |
| S42 | Ring | 1 |
```
