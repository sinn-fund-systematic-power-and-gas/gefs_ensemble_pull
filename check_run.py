#!/usr/bin/env python3
"""
Quick check of a GEFS output parquet: schema, row count, members, steps.
Usage: python check_run.py [path_to.parquet]
  If no path given, uses the most recent file in output/.
"""
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = SCRIPT_DIR / "output"

import pandas as pd

def main():
    if len(sys.argv) > 1:
        path = Path(sys.argv[1])
    else:
        parquets = sorted(OUTPUT_DIR.glob("gefs_temps_*.parquet"), key=lambda p: p.stat().st_mtime, reverse=True)
        if not parquets:
            print("No parquet files in output/")
            return
        path = parquets[0]
        print(f"Latest file: {path.name}\n")

    if not path.exists():
        print(f"Not found: {path}")
        return

    df = pd.read_parquet(path)
    print("Schema:", list(df.columns))
    print("Rows:", len(df))
    print("\nMembers:", sorted(df["member"].unique().tolist()))
    print("Member count:", df["member"].nunique())
    print("Cities:", df["city"].nunique())
    print("Forecast hours:", df["forecast_hour"].nunique())
    expected = 105 * 10 * 32
    ok = "OK" if len(df) == expected else f"expected {expected}"
    print(f"\nExpected 33,600 (105×10×32): {len(df)} — {ok}")

if __name__ == "__main__":
    main()
