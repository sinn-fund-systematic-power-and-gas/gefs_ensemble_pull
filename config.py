"""
Standalone config for GEFS ensemble historical pull.
All paths are relative to this folder.
"""

import os

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(_THIS_DIR, "output")
HERBIE_SAVE_DIR = os.path.join(_THIS_DIR, ".herbie_cache")

# GEFS time resolution: 0–240h every 3h, 240–384h every 6h (reduces storage/lower reliability after day 10)
FORECAST_HOURS = 384
# Steps to request: 0, 3, ..., 240 (3h) then 246, 252, ..., 384 (6h)
FORECAST_HOURS_LIST = list(range(0, 241, 3)) + list(range(246, 385, 6))
GEFS_RUN_HOURS = [0, 6, 12, 18]

# GEFS ensemble configuration: all 31 members (control 0 + perturbed 1–30) + mean
GEFS_PRODUCT = "atmos.5"  # half-degree atmospheric fields
# Herbie: 0 or "c00" = control, 1–30 or "p01"–"p30" = perturbed, "mean"/"avg" = ensemble mean
GEFS_MEMBERS = ["mean"] + list(range(0, 31))  # mean + control(0) + p01..p30
HERBIE_PRIORITY = ["aws", "nomads"]

# 10 US gas demand cities (lat/lon) — same as GFS project
CITIES = {
    "new_york": {"lat": 40.71, "lon": -74.01},
    "boston": {"lat": 42.36, "lon": -71.06},
    "philadelphia": {"lat": 39.95, "lon": -75.17},
    "chicago": {"lat": 41.88, "lon": -87.63},
    "detroit": {"lat": 42.33, "lon": -83.05},
    "houston": {"lat": 29.76, "lon": -95.37},
    "dallas": {"lat": 32.78, "lon": -96.80},
    "atlanta": {"lat": 33.75, "lon": -84.39},
    "denver": {"lat": 39.74, "lon": -104.99},
    "los_angeles": {"lat": 34.05, "lon": -118.24},
}

BOUNDS = {"lat": (28.5, 43.5), "lon": (-119.5, -70.0)}

