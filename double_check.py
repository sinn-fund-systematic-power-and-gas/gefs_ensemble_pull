"""
Check and fill gaps in GEFS output parquet files for this project.

New layout (matches run_fetch_all.py):
- Parquets live under: OUTPUT_DIR / YYYYMMDD /
- One file per member per run hour:
    gefs_temps_YYYYMMDD_HHz_member_{member}.parquet

This script:
- Detects missing / incomplete runs across all members.
- Refetches any gaps and writes/merges per-member files back into day folders.

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
from config import (
    CITIES,
    GEFS_RUN_HOURS,
    OUTPUT_DIR,
    FORECAST_HOURS_LIST,
    GEFS_MEMBERS,
)
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


def _member_file_path(day_dir: Path, label: str, member: str) -> Path:
    """Return the expected path for a single member parquet for a given run label."""
    return day_dir / f"gefs_temps_{label}z_member_{member}.parquet"


def find_gaps(start: datetime, end: datetime, output_dir: Path):
    """
    Scan OUTPUT_DIR for missing or incomplete runs in the new per-day/per-member layout.

    A run is considered:
    - missing: no member files exist for its day/hour.
    - incomplete: some members or forecast hours are missing (total rows < expected).
    """
    missing, incomplete = [], []

    for run_time, label, expected_rows in get_expected_runs(start, end):
        day_dir = output_dir / run_time.strftime("%Y%m%d")
        if not day_dir.exists():
            missing.append((run_time, label, expected_rows))
            continue

        total_rows = 0
        member_files_found = 0
        for member in GEFS_MEMBERS:
            member_str = str(member)
            path = _member_file_path(day_dir, label, member_str)
            if not path.exists():
                continue
            try:
                df = pd.read_parquet(path)
                member_files_found += 1
                total_rows += len(df)
            except Exception as e:
                print(f"Warning: Could not read {path.name}: {e}")

        if member_files_found == 0:
            missing.append((run_time, label, expected_rows))
        elif total_rows < expected_rows or member_files_found < len(GEFS_MEMBERS):
            incomplete.append((run_time, label, expected_rows, total_rows))

    return missing, incomplete


def main():
    parser = argparse.ArgumentParser(
        description="Check and fill gaps in GEFS per-member/parquet output under output/YYYYMMDD/"
    )
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
        day_dir = output_dir / run_time.strftime("%Y%m%d")
        day_dir.mkdir(parents=True, exist_ok=True)
        print(f"  Filling {label} -> day folder {day_dir}")

        try:
            df = fetch_full_run(run_time)
        except Exception as e:
            print(f"    Error fetching {label}: {e}")
            continue

        if df.empty:
            print(f"    No data for {label}, skipping.")
            continue

        df = df.sort_values(["city", "member", "forecast_hour"]).reset_index(drop=True)

        written_files = 0
        total_rows_written = 0
        for member_value, member_df in df.groupby("member"):
            member_str = str(member_value)
            out_path = _member_file_path(day_dir, label, member_str)

            if out_path.exists():
                try:
                    existing_df = pd.read_parquet(out_path)
                    member_df = pd.concat([existing_df, member_df], ignore_index=True)
                    member_df = member_df.drop_duplicates(
                        subset=["vintage_time", "forecast_hour", "city", "member"],
                        keep="last",
                    )
                except Exception as e:
                    print(
                        f"    Warning: Could not merge with existing {out_path.name}: {e}"
                    )

            member_df.to_parquet(out_path, index=False)
            written_files += 1
            total_rows_written += len(member_df)

        print(
            f"    Wrote {total_rows_written} rows into {written_files} member files in {day_dir}"
        )


if __name__ == "__main__":
    main()

