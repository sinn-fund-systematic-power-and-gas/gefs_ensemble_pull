"""
Fetch one full GEFS run (all 31 members + mean): 0–384h (3h/6h steps), all 10 cities.
Returns DataFrame: vintage_time, forecast_hour, valid_time, city, member, temp_k, temp_f.
Standalone — only imports from this folder's config.
"""

import warnings
from datetime import datetime, timedelta
from typing import Optional, Union

import pandas as pd
import xarray as xr
from herbie import Herbie

from config import (
    BOUNDS,
    CITIES,
    FORECAST_HOURS,
    FORECAST_HOURS_LIST,
    GEFS_MEMBERS,
    GEFS_PRODUCT,
    HERBIE_PRIORITY,
    HERBIE_SAVE_DIR,
)

warnings.filterwarnings("ignore", category=FutureWarning, module="cfgrib")


def _kelvin_to_fahrenheit(temp_k: float) -> float:
    return (temp_k - 273.15) * 9 / 5 + 32


def _get_temp_var(ds: xr.Dataset) -> str:
    for var in ds.data_vars:
        if "TMP" in var or "t2m" in var.lower():
            return var
    raise ValueError("No temperature variable in dataset")


def _extract_point_temp_k(ds: xr.Dataset, lat: float, lon: float) -> float:
    temp_var = _get_temp_var(ds)
    lon_360 = lon + 360 if lon < 0 else lon
    val = ds[temp_var].sel(latitude=lat, longitude=lon_360, method="nearest")
    return float(val.values)


def _naive_utc(dt: datetime) -> datetime:
    if hasattr(dt, "tzinfo") and dt.tzinfo is not None and getattr(dt, "utcoffset", lambda: None)():
        dt = (dt - dt.utcoffset()).replace(tzinfo=None)
    elif hasattr(dt, "tzinfo") and dt.tzinfo is not None:
        dt = dt.replace(tzinfo=None)
    return dt


def fetch_one_hour(
    vintage_time: datetime, fxx: int, member: Union[str, int]
) -> Optional[xr.Dataset]:
    """Fetch one GEFS forecast hour for one member (mean, 0..30). Returns None on failure."""
    try:
        H = Herbie(
            date=vintage_time,
            model="gefs",
            product=GEFS_PRODUCT,
            member=member,
            fxx=fxx,
            priority=HERBIE_PRIORITY,
            verbose=False,
            overwrite=True,
            save_dir=HERBIE_SAVE_DIR,
        )
        H.download(overwrite=True)
        H = Herbie(
            date=vintage_time,
            model="gefs",
            product=GEFS_PRODUCT,
            member=member,
            fxx=fxx,
            priority=HERBIE_PRIORITY,
            verbose=False,
            save_dir=HERBIE_SAVE_DIR,
        )
        ds = H.xarray(search="TMP:2 m", remove_grib=True)
        if isinstance(ds, list):
            ds = ds[0]
        lat_min, lat_max = BOUNDS["lat"]
        lon_min, lon_max = BOUNDS["lon"]
        lon_min = lon_min + 360 if lon_min < 0 else lon_min
        lon_max = lon_max + 360 if lon_max < 0 else lon_max
        ds = ds.sel(
            latitude=slice(lat_max, lat_min),
            longitude=slice(lon_min, lon_max),
        )
        return ds
    except Exception:
        return None


def fetch_full_run(
    vintage_time: datetime,
    progress_callback: Optional[callable] = None,
) -> pd.DataFrame:
    """
    Fetch GEFS steps for one vintage, all GEFS_MEMBERS (mean + 0..30), all 10 cities.
    0–240h every 3h, 246–384h every 6h. progress_callback(fxx, total_steps) optional.
    """
    vintage_time = _naive_utc(vintage_time)
    steps = FORECAST_HOURS_LIST
    total_steps = len(steps)
    rows = []

    for fxx in steps:
        for member in GEFS_MEMBERS:
            ds = fetch_one_hour(vintage_time, fxx, member)
            if ds is None:
                continue
            valid_time = vintage_time + timedelta(hours=fxx)
            member_str = str(member)  # "mean", "0", "1", ..., "30"
            for city_id, info in CITIES.items():
                lat, lon = info["lat"], info["lon"]
                try:
                    temp_k = _extract_point_temp_k(ds, lat, lon)
                    temp_f = _kelvin_to_fahrenheit(temp_k)
                    rows.append(
                        {
                            "vintage_time": vintage_time,
                            "forecast_hour": fxx,
                            "valid_time": valid_time,
                            "city": city_id,
                            "member": member_str,
                            "temp_k": temp_k,
                            "temp_f": temp_f,
                        }
                    )
                except Exception:
                    pass
        if progress_callback:
            progress_callback(fxx, total_steps)

    return pd.DataFrame(rows)

