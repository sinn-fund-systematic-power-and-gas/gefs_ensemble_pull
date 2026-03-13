"""
Check and fill gaps in output/ parquet files for GEFS pulls (all members + mean).
Does not delete — merges missing forecast hours/members.
Run from this folder: python double_check.py --start YYYY-MM-DD --end YYYY-MM-DD
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
from config import CITIES, GEFS_RUN_HOURS, OUTPUT_DIR, FORECAST_HOURS_LIST, GEFS_MEMBERS
from fetch_full_run import fetch_full_run


def parse_date(s: str) -> datetime:
    return datetime.strptime(s.strip(), "%Y-%m-%d")


def get_expected_runs(start: datetime, end: datetime):
    expected_rows = len(FORECAST_HOURS_LIST) * len(CITIES) * len(GEFS_MEMBERS)
    current = start.replace(hour=0, minute=0, second=0, microsecond=0)
    end_day = end.replace(hour=0, minute=0, second=0, microsecond=0)
    while current <= end_day:
        for h in GEFS_RUN_HOURS:
            run_time = current.replace(hour=h, minute=0, second=0, microsecond=0)
            label = run_time.strftime("%Y%m%d_%H")
            yield run_time, label, expected_rows
        current += timedelta(days=1)


def scan_existing_files(output_dir: Path):
    existing = {}
    for parquet_file in output_dir.glob("gefs_temps_*.parquet"):
        try:
            label = parquet_file.stem.replace("gefs_temps_", "").replace("z", "")
            df = pd.read_parquet(parquet_file)
            existing[label] = len(df)
        except Exception as e:
            print(f"Warning: Could not read {parquet_file.name}: {e}")
    return existing


def find_gaps(start: datetime, end: datetime, output_dir: Path):
    existing = scan_existing_files(output_dir)
    missing, incomplete = [], []
    for run_time, label, expected_rows in get_expected_runs(start, end):
        if label not in existing:
            missing.append((run_time, label, expected_rows))
        elif existing[label] < expected_rows:
            incomplete.append((run_time, label, expected_rows, existing[label]))
    return missing, incomplete


def main():
    parser = argparse.ArgumentParser(description="Check and fill gaps in GEFS output/ parquets")
    g = parser.add_mutually_exclusive_group(required=True)
    g.add_argument("--start", type=str)
    g.add_argument("--months", type=str, nargs="+")
    parser.add_argument("--end", type=str)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    output_dir = Path(OUTPUT_DIR)
    output_dir.mkdir(parents=True, exist_ok=True)

    if args.months:
        start_dates = []
        end_dates = []
        for month_str in args.months:
            y, m = int(month_str[:4]), int(month_str[4:6])
            start_dates.append(datetime(y, m, 1))
            end_dates.append(
                (datetime(y, m + 1, 1) - timedelta(days=1))
                if m < 12
                else (datetime(y + 1, 1, 1) - timedelta(days=1))
            )
        start, end = min(start_dates), max(end_dates)
    else:
        if not args.start or not args.end:
            parser.error("--start and --end required unless using --months")
        start = parse_date(args.start)
        end = parse_date(args.end)

    if start > end:
        parser.error("--start must be <= --end")

    print(f"Checking runs from {start.date()} to {end.date()} in {output_dir}")
    missing, incomplete = find_gaps(start, end, output_dir)

    print("\nMissing runs:")
    for run_time, label, expected_rows in missing:
        print(f"  {label} (expected {expected_rows} rows)")

    print("\nIncomplete runs:")
    for run_time, label, expected_rows, actual_rows in incomplete:
        print(f"  {label}: {actual_rows}/{expected_rows} rows")

    if args.dry_run:
        print("\nDry run only. Not filling gaps.")
        return

    to_fill = missing + [(rt, label, exp) for rt, label, exp, _ in incomplete]
    if not to_fill:
        print("\nNo gaps to fill.")
        return

    print(f"\nFilling {len(to_fill)} runs...")
    for run_time, label, expected_rows in to_fill:
        out_path = output_dir / f"gefs_temps_{label}z.parquet"
        print(f"  Filling {label} -> {out_path.name}")
        try:
            df = fetch_full_run(run_time)
        except Exception as e:
            print(f"    Error fetching {label}: {e}")
            continue

        if df.empty:
            print(f"    No data for {label}, skipping.")
            continue

        if out_path.exists():
            try:
                existing_df = pd.read_parquet(out_path)
                df = pd.concat([existing_df, df], ignore_index=True)
                df = df.drop_duplicates(
                    subset=["vintage_time", "forecast_hour", "city", "member"], keep="last"
                )
            except Exception as e:
                print(f"    Warning: Could not merge with existing {out_path.name}: {e}")

        df = df.sort_values(["city", "member", "forecast_hour"]).reset_index(drop=True)
        df.to_parquet(out_path, index=False)
        print(f"    Wrote {len(df)} rows -> {out_path.name}")


if __name__ == "__main__":
    main()

