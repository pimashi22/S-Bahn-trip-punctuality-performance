# Task 1 — Dataset Selection

## Dataset Overview

**Name:** Berlin S-Bahn Punctuality Database  
**Source:** Kaggle  
**URL:** https://www.kaggle.com/datasets/alperenmyung/berlin-s-bahn-punctuality-database  
**Period:** January 1, 2024 — December 30, 2024  
**Total Records:** 131,771 trips  

---

## Why This Dataset Was Selected

This dataset was selected for the following reasons:

**Novel OLTP Scenario** — The dataset represents a real-world urban railway operations system. It is not an OLAP dataset and does not contain pre-built facts or dimensions. The raw operational data requires full dimensional modelling and transformation work to become analytically useful.

**Sufficient Volume** — With 131,771 trip records covering a full calendar year (2024), the dataset provides enough data to produce meaningful aggregations, hierarchies, and analytical reports.

**Rich Analytical Potential** — The dataset supports a wide range of DW/BI analytical tasks including delay pattern analysis by line, station, time of day, weather condition, and infrastructure incident type.

**Multiple Source Types** — The dataset can be naturally extended with a second source type. Real meteorological data from the Open-Meteo Historical Weather API was used as a SQL Server database source, satisfying the two-source-type requirement.

**Hierarchy Support** — The data naturally supports hierarchies such as Date (Day → Month → Quarter → Year), Location (Station → Line → Network), and Delay (None → Minor → Moderate → Severe).

---

## Business Process

> *Monitoring and analysing S-Bahn trip punctuality performance across lines, stations, time periods, weather conditions, and infrastructure incidents.*

### Key Analytical Questions
- Which lines have the highest delay rates?
- How does weather affect punctuality?
- Which stations experience the most delays?
- What is the impact of infrastructure incidents?
- How does punctuality vary between peak and off-peak hours?
- What percentage of trips are cancelled, delayed, or on time?

---

## Source Files Description

### trips.csv — 131,771 rows
The main operational table. Each row represents one S-Bahn trip.

| Column | Type | Description |
|--------|------|-------------|
| trip_id | INT | Unique trip identifier (0–131,770) |
| line_id | VARCHAR | S-Bahn line (S1/S2/S5/S7/S41/S42) |
| start_station_id | VARCHAR | Departure station ID |
| end_station_id | VARCHAR | Arrival station ID |
| scheduled_departure_time | DATETIME | Planned departure timestamp |
| is_peak_hour | BOOLEAN | True if trip departs during peak hours |
| is_delayed | BOOLEAN | True if trip was delayed |
| delay_minutes | INT | Minutes of delay (range: 0–1,181) |
| is_cancelled | BOOLEAN | True if trip was cancelled |

### lines.csv — 6 rows
Reference data for S-Bahn lines.

| Column | Type | Description |
|--------|------|-------------|
| line_id | VARCHAR | Line identifier |
| line_name | VARCHAR | Display name |
| is_ring_line | BOOLEAN | True for ring lines S41 and S42 |
| delay_propensity | FLOAT | Historical delay likelihood (0.60–1.00) |

**Lines:** S1, S2, S5, S7, S41 (Ring), S42 (Ring)

### stations.csv — 10 rows
Reference data for S-Bahn stations.

| Column | Type | Description |
|--------|------|-------------|
| station_id | VARCHAR | Station identifier |
| name | VARCHAR | Full station name |
| is_major_hub | BOOLEAN | True for major interchange stations |
| location_category | VARCHAR | city_center / suburb / airport / event_venue |

**Stations include:** Berlin Hauptbahnhof, Berlin Ostkreuz, Potsdam Hauptbahnhof, Friedrichstraße, Olympiastadion, Flughafen BER, Alexanderplatz, Charlottenburg, Berlin Südkreuz, Berlin-Spandau

### incidents.csv — 36 rows
Infrastructure incident records affecting operations.

| Column | Type | Description |
|--------|------|-------------|
| incident_id | VARCHAR | Unique incident identifier |
| timestamp | DATETIME | When the incident occurred |
| incident_type | VARCHAR | Signal Failure / Track Maintenance / Technical Fault / Power Outage |
| delay_impact_factor | FLOAT | Severity multiplier (1.01–20.99) |
| line_id | VARCHAR | Affected line |

### weather.csv — 8,761 rows
Hourly weather observations from Kaggle source.

| Column | Type | Description |
|--------|------|-------------|
| timestamp | DATETIME | Hourly timestamp |
| temperature_c | FLOAT | Temperature in Celsius (-4.18 to 34.63) |
| precipitation_mm | FLOAT | Precipitation in mm (0.00 to 14.44) |
| wind_speed_kmh | FLOAT | Wind speed in km/h (0.00 to 54.42) |
| weather_condition | VARCHAR | Clear / Rainy / Stormy |

---

## Entity Relationship Diagram

```
STATIONS ──────────────────────────────────────┐
station_id PK                                  │
name                                           │
is_major_hub                                   │
location_category                              │
                                               │
LINES ─────────────────────────────────────┐   │
line_id PK                                 │   │
line_name                                  │   │
is_ring_line                               │   │
delay_propensity                           │   │
           │                               │   │
           │ 1:M                           │ M:1 │ M:1
           ▼                               ▼   ▼
         TRIPS ──────────────────────────────────
         trip_id PK
         line_id FK ──────────────────────► LINES
         start_station_id FK ─────────────► STATIONS
         end_station_id FK ───────────────► STATIONS
         scheduled_departure_time
         is_peak_hour
         is_delayed
         delay_minutes
         is_cancelled

INCIDENTS ─────────────────────────────────────
incident_id PK
timestamp
incident_type
delay_impact_factor
line_id FK ───────────────────────────────► LINES

WEATHER ───────────────────────────────────────
timestamp PK
temperature_c
precipitation_mm
wind_speed_kmh
weather_condition
```

---

## Data Quality Summary

| File | Null Values | Duplicates | Data Issues |
|------|-------------|------------|-------------|
| trips.csv | 0 | 0 | BIT columns stored as True/False text |
| lines.csv | 0 | 0 | None |
| stations.csv | 0 | 0 | Station name column called "name" |
| incidents.csv | 0 | 0 | timestamp is reserved word — handled in ETL |
| weather.csv | 0 | 0 | Limited conditions (Clear/Rainy/Stormy only) |

All data quality issues were handled during the ETL transformation phase.

---

## Dataset Statistics

| Metric | Value |
|--------|-------|
| Total trips | 131,771 |
| On-time trips (0 delay) | 25,083 (19.0%) |
| Minor delays (1–5 min) | 101,950 (77.4%) |
| Moderate delays (6–15 min) | 1,848 (1.4%) |
| Severe delays (>15 min) | 2,890 (2.2%) |
| Cancelled trips | 860 (0.7%) |
| Max delay recorded | 1,181 minutes |
| Cross-city trips | 84,916 (64.4%) |
| Local trips | 46,855 (35.6%) |
