"""
Re-pull specific (fxx, member) holes for one run and merge into existing parquets.
Use when you know specific (fxx, member) holes to re-pull from NOAA (e.g. from double_check or manual inspection).

Usage:
  python repull_holes.py --date 2026-01-09 --hour 0 --fxx 336 --members 1,2,3,4
  python repull_holes.py --date 2026-01-09 --hour 0 --fxx 336 --members 1 2 3 4
"""

import argparse
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
os.chdir(SCRIPT_DIR)
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import pandas as pd

from config import OUTPUT_DIR
from double_check import _member_file_path
from fetch_full_run import (
    _extract_point_temp_k,
    _kelvin_to_fahrenheit,
    _naive_utc,
    fetch_one_hour,
)
from config import CITIES


def parse_date(s: str) -> datetime:
    return datetime.strptime(s.strip(), "%Y-%m-%d")


def main():
    parser = argparse.ArgumentParser(
        description="Re-pull specific fxx/members for one run and merge into existing parquets."
    )
    parser.add_argument("--date", type=str, required=True, metavar="YYYY-MM-DD")
    parser.add_argument("--hour", type=int, default=0, choices=[0, 6, 12, 18], help="Run hour (0, 6, 12, 18)")
    parser.add_argument("--fxx", type=int, required=True, nargs="+", help="Forecast hour(s) to repull (e.g. 336)")
    parser.add_argument(
        "--members",
        type=str,
        required=True,
        nargs="+",
        help="Member(s) to repull (e.g. 1 2 3 4 or 1,2,3,4)",
    )
    args = parser.parse_args()

    # Flatten and parse members (allow "1,2,3,4" or "1" "2" "3" "4")
    members = []
    for m in args.members:
        members.extend(str(x).strip() for x in m.split(","))
    members = list(dict.fromkeys(members))  # unique, order preserved

    vintage_time = parse_date(args.date).replace(
        hour=args.hour, minute=0, second=0, microsecond=0
    )
    vintage_time = _naive_utc(vintage_time)
    label = vintage_time.strftime("%Y%m%d_%H")  # e.g. 20260109_00

    day_dir = Path(OUTPUT_DIR) / vintage_time.strftime("%Y%m%d")
    if not day_dir.exists():
        print(f"Day dir not found: {day_dir}")
        return

    print(f"Re-pulling {label}: fxx={args.fxx}, members={members}")
    rows_by_member = {m: [] for m in members}
    valid_time_base = vintage_time
    missing = []  # (fxx, member) that could not be fetched

    for fxx in args.fxx:
        valid_time = valid_time_base + timedelta(hours=fxx)
        for member_str in members:
            member_val = int(member_str) if member_str.isdigit() else member_str
            ds = fetch_one_hour(vintage_time, fxx, member_val)
            if ds is None:
                missing.append((fxx, member_str))
                print(f"  MISSING (could not get): fxx={fxx} member={member_str}")
                continue
            for city_id, info in CITIES.items():
                lat, lon = info["lat"], info["lon"]
                try:
                    temp_k = _extract_point_temp_k(ds, lat, lon)
                    temp_f = _kelvin_to_fahrenheit(temp_k)
                    rows_by_member[member_str].append(
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
            print(f"  Got fxx={fxx} member={member_str}")

    if missing:
        print(f"\n--- Could not get (data missing from source) ---")
        for fxx, member_str in missing:
            print(f"  fxx={fxx} member={member_str}")
        print(f"  Total: {len(missing)} (fxx, member) pairs not available.")

    total_new = 0
    for member_str, rows in rows_by_member.items():
        if not rows:
            continue
        path = _member_file_path(day_dir, label, member_str)
        if not path.exists():
            print(f"  Skip {member_str}: no existing file {path.name}")
            continue
        new_df = pd.DataFrame(rows)
        existing = pd.read_parquet(path)
        merged = pd.concat([existing, new_df], ignore_index=True)
        merged = merged.drop_duplicates(
            subset=["vintage_time", "forecast_hour", "city", "member"], keep="last"
        )
        merged = merged.sort_values(["city", "forecast_hour"]).reset_index(drop=True)
        merged.to_parquet(path, index=False)
        total_new += len(rows)
        print(f"  Wrote {len(rows)} rows -> {path.name}")

    print(f"\nDone. Merged {total_new} new rows into {day_dir}")
    if missing and total_new == 0:
        print("(No data could be merged; all requested (fxx, member) were missing from source.)")


if __name__ == "__main__":
    main()
