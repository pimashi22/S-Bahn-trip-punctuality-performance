
# Staging Tables — BerlinSBahn_Staging

## Tables Created Automatically via SSIS

| Table | Source | Source Type | Rows |
|-------|--------|-------------|------|
| stg_lines | lines.csv | Flat File (CSV) | 6 |
| stg_stations | stations.csv | Flat File (CSV) | 10 |
| stg_incidents | incidents.csv | Flat File (CSV) | 36 |
| stg_weather_kaggle | weather.csv | Flat File (CSV) | 8761 |
| stg_trips | trips.csv | Flat File (CSV) | 131771 |
| stg_weather_openmeteo | BerlinSBahn_Source DB | SQL Server DB | 8784 |

## Notes
- Staging tables created automatically using SSIS 
  OLE DB Destination "New" button
- No manual CREATE TABLE needed for staging
- All BIT columns stored as VARCHAR(10) in staging 
  (True/False text) — converted during DW load
- timestamp columns stored as VARCHAR(30) in staging 
  — converted to DATETIME during DW load
- Event Handler OnPreExecute added to each task 
  to TRUNCATE table before each load
