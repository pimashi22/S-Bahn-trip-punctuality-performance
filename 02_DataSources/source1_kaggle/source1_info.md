# Source 1 — Kaggle Berlin S-Bahn Dataset
**Source Type:** Flat Files (CSV)  
**Dataset:** Berlin S-Bahn Punctuality Database  
**URL:** https://www.kaggle.com/datasets/alperenmyung/berlin-s-bahn-punctuality-database  
**Period:** January 1, 2024 — December 30, 2024  

---

## Files Overview

| File | Rows | Description |
|------|------|-------------|
| trips.csv | 131,771 | Main fact source — all S-Bahn trips |
| lines.csv | 6 | S-Bahn line details |
| stations.csv | 10 | Station information |
| incidents.csv | 36 | Infrastructure incidents |
| weather.csv | 8,761 | Hourly Kaggle weather data |

---

## File Schemas

### trips.csv
| Column | Type | Description |
|--------|------|-------------|
| trip_id | INT | Unique trip identifier |
| line_id | VARCHAR | S-Bahn line (S1/S2/S5/S7/S41/S42) |
| start_station_id | VARCHAR | Departure station |
| end_station_id | VARCHAR | Arrival station |
| scheduled_departure_time | DATETIME | Planned departure |
| is_peak_hour | BOOLEAN | Peak hour flag |
| is_delayed | BOOLEAN | Delay flag |
| delay_minutes | INT | Minutes delayed (0-1181) |
| is_cancelled | BOOLEAN | Cancellation flag |

### lines.csv
| Column | Type | Description |
|--------|------|-------------|
| line_id | VARCHAR | Line identifier |
| line_name | VARCHAR | Display name |
| is_ring_line | BOOLEAN | Ring line flag |
| delay_propensity | FLOAT | Delay likelihood (0.6-1.0) |

### stations.csv
| Column | Type | Description |
|--------|------|-------------|
| station_id | VARCHAR | Station identifier |
| name | VARCHAR | Station name |
| is_major_hub | BOOLEAN | Major hub flag |
| location_category | VARCHAR | city_center/suburb/airport/event_venue |

### incidents.csv
| Column | Type | Description |
|--------|------|-------------|
| incident_id | VARCHAR | Incident identifier |
| timestamp | DATETIME | When incident occurred |
| incident_type | VARCHAR | Signal Failure/Track Maintenance etc |
| delay_impact_factor | FLOAT | Impact multiplier (1.01-20.99) |
| line_id | VARCHAR | Affected line |

### weather.csv (Kaggle)
| Column | Type | Description |
|--------|------|-------------|
| timestamp | DATETIME | Hourly timestamp |
| temperature_c | FLOAT | Temperature in Celsius |
| precipitation_mm | FLOAT | Precipitation |
| wind_speed_kmh | FLOAT | Wind speed |
| weather_condition | VARCHAR | Clear/Rainy/Stormy |
