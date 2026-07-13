python# ============================================================
# FILE: fetch_weather.py
# PURPOSE: Fetch Berlin hourly weather data from 
#          Open-Meteo Historical Weather API
# Run in Google Colab or locally with requests + pandas
# Output: weather_openmeteo.csv (8784 rows)
# ============================================================

import requests
import pandas as pd

# API Configuration
url = "https://archive-api.open-meteo.com/v1/archive"

params = {
    "latitude": 52.52,
    "longitude": 13.41,
    "start_date": "2024-01-01",
    "end_date": "2024-12-31",
    "hourly": [
        "temperature_2m",
        "precipitation",
        "wind_speed_10m",
        "weather_code"
    ],
    "timezone": "Europe/Berlin"
}

# Weather code mapping function
def map_weather_code(code):
    if code == 0:
        return "Clear"
    elif code in [1, 2]:
        return "Partly Cloudy"
    elif code == 3:
        return "Cloudy"
    elif code in [51, 53, 55, 61, 63, 65, 80, 81, 82]:
        return "Rainy"
    elif code in [71, 73, 75, 77, 85, 86]:
        return "Snowy"
    elif code in [95, 96, 99]:
        return "Stormy"
    else:
        return "Cloudy"

# Fetch data
print("Fetching weather data from Open-Meteo API...")
response = requests.get(url, params=params)
data = response.json()

# Build DataFrame
hourly = data["hourly"]
df = pd.DataFrame({
    "timestamp": hourly["time"],
    "temperature_c": hourly["temperature_2m"],
    "precipitation_mm": hourly["precipitation"],
    "wind_speed_kmh": hourly["wind_speed_10m"],
    "weather_condition": [
        map_weather_code(c) for c in hourly["weather_code"]
    ]
})

# Format timestamp
df["timestamp"] = pd.to_datetime(df["timestamp"]) \
                    .dt.strftime("%Y-%m-%d %H:%M:%S")

# Save to CSV
df.to_csv("weather_openmeteo.csv", index=False)
print(f"Saved {len(df)} rows to weather_openmeteo.csv")
print(df.head())

# Download in Google Colab
try:
    from google.colab import files
    files.download("weather_openmeteo.csv")
    print("Download started!")
except ImportError:
    print("Not running in Colab.")
    print("File saved locally as weather_openmeteo.csv")
```
