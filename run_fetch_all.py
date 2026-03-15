"""
Fetch GEFS 2m temperature: 16 days (0–384h), 3h steps 0–240h / 6h steps 240–384h, all 10 cities.

New output layout:
- Day folders under: output/YYYYMMDD/
- One parquet per member per run hour:
    gefs_temps_YYYYMMDD_HHz_member_{member}.parquet

Run from this folder's parent:
  python -m gefs_ensemble_pull.run_fetch_all --start 2025-01-01 --end 2025-01-02
Or from inside this folder:
  python run_fetch_all.py --start 2025-01-01 --end 2025-01-02

Usage:
  # Latest run only
  python run_fetch_all.py --latest 1

  # Date range (all 00z, 06z, 12z, 18z)
  python run_fetch_all.py --start 2026-02-01 --end 2026-02-12

  # One run per day (00z only)
  python run_fetch_all.py --start 2026-02-18 --end 2026-02-20 --hour 0
"""

import argparse
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
import shutil

# Make this folder the working directory and add it to path (works when folder is anywhere)
SCRIPT_DIR = Path(__file__).resolve().parent
os.chdir(SCRIPT_DIR)
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import pandas as pd
from config import GEFS_RUN_HOURS, OUTPUT_DIR, FORECAST_HOURS, HERBIE_SAVE_DIR, GEFS_MEMBERS
from fetch_full_run import fetch_full_run
from gefs_utils import get_latest_gefs_run, get_previous_gefs_run


def parse_date(s: str) -> datetime:
    return datetime.strptime(s.strip(), "%Y-%m-%d")


def get_run_times_from_range(start: datetime, end: datetime, hours: list[int] | None = None):
    """Yield (run_time, label) for each run from start through end."""
    if hours is None:
        hours = GEFS_RUN_HOURS
    current = start.replace(hour=0, minute=0, second=0, microsecond=0)
    end_day = end.replace(hour=0, minute=0, second=0, microsecond=0)
    while current <= end_day:
        for h in hours:
            run_time = current.replace(hour=h, minute=0, second=0, microsecond=0)
            label = run_time.strftime("%Y%m%d_%H")
            yield run_time, label
        current += timedelta(days=1)


def get_latest_run_times(n: int):
    """Yield (run_time, label) for the last n GEFS runs."""
    run_time = get_latest_gefs_run()
    for _ in range(n):
        label = run_time.strftime("%Y%m%d_%H")
        yield run_time, label
        run_time = get_previous_gefs_run(run_time)


def run_already_complete(run_time: datetime, label: str) -> bool:
    """True if all 32 member parquets exist for this run."""
    day_dir = Path(OUTPUT_DIR) / run_time.strftime("%Y%m%d")
    if not day_dir.exists():
        return False
    for member in GEFS_MEMBERS:
        member_str = str(member)
        path = day_dir / f"gefs_temps_{label}z_member_{member_str}.parquet"
        if not path.exists():
            return False
    return True


def cleanup_herbie_cache_for_run(vintage: datetime) -> None:
    """
    Remove the Herbie cache directory for a single GEFS vintage date,
    without touching cache for other dates. Herbie stores files under:
      HERBIE_SAVE_DIR / "gefs" / YYYYMMDD
    """
    date_str = vintage.strftime("%Y%m%d")
    cache_dir = Path(HERBIE_SAVE_DIR) / "gefs" / date_str
    if cache_dir.exists():
        try:
            shutil.rmtree(cache_dir)
            print(f"  Cleared Herbie cache for {date_str}: {cache_dir}")
        except Exception as e:
            print(f"  Warning: could not clear cache {cache_dir}: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="Fetch GEFS 2m temp: all 32 members, 16 days, 10 cities → output/"
    )
    g = parser.add_mutually_exclusive_group(required=True)
    g.add_argument("--latest", type=int, metavar="N", help="Fetch latest N runs only")
    g.add_argument("--start", type=str, metavar="YYYY-MM-DD", help="Start date (inclusive)")
    parser.add_argument("--end", type=str, metavar="YYYY-MM-DD", help="End date (required if --start)")
    parser.add_argument(
        "--hour",
        type=int,
        default=None,
        metavar="H",
        choices=[0, 6, 12, 18],
        help="If set, fetch only this run hour each day",
    )
    args = parser.parse_args()

    if args.latest is not None:
        runs = list(get_latest_run_times(args.latest))
    else:
        if not args.end:
            parser.error("--end required when using --start")
        start = parse_date(args.start)
        end = parse_date(args.end)
        if start > end:
            parser.error("--start must be <= --end")
        hours = [args.hour] if args.hour is not None else None
        runs = list(get_run_times_from_range(start, end, hours=hours))

    if not runs:
        print("No runs to fetch.")
        return

    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    print(f"Output directory: {OUTPUT_DIR}")
    print(f"Runs to fetch: {len(runs)}")
    for run_time, label in runs:
        print(f"  {run_time.strftime('%Y-%m-%d %H:%M')} UTC -> day folder with member files")
    print()

    for run_time, label in runs:
        if run_already_complete(run_time, label):
            print(f"  Already have {label}, skipping.")
            continue
        print(f"Fetching {label}...")

        def progress(fxx, total):
            if fxx % 48 == 0 or fxx == FORECAST_HOURS:
                print(f"  Hour {fxx}/{FORECAST_HOURS}")

        try:
            df = fetch_full_run(run_time, progress_callback=progress)
        except Exception as e:
            print(f"  Error: {e}")
            continue

        if df.empty:
            print(f"  No data for {label}, skipping.")
            continue

        # Sort once, then split by member to write one file per member.
        df = df.sort_values(["city", "member", "forecast_hour"]).reset_index(drop=True)

        day_dir = Path(OUTPUT_DIR) / run_time.strftime("%Y%m%d")
        day_dir.mkdir(parents=True, exist_ok=True)

        written_files = 0
        for member_value, member_df in df.groupby("member"):
            member_str = str(member_value)
            member_path = day_dir / f"gefs_temps_{label}z_member_{member_str}.parquet"
            member_df.to_parquet(member_path, index=False)
            written_files += 1

        print(
            f"  Wrote {len(df)} rows into {written_files} member files in {day_dir}"
        )

        # Free disk space by removing only the Herbie cache for this vintage date.
        cleanup_herbie_cache_for_run(run_time)

    print("\nDone.")


if __name__ == "__main__":
    main()

