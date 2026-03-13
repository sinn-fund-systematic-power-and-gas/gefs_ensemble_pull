## GEFS Ensemble Historical Pull (standalone)

Pull **NOAA GEFS 2m temperature** for 16 days (0–384h) for **10 US cities**: **all 31 members** (control + perturbed 1–30) **plus the ensemble mean** (32 total). Time steps: **3-hour 0–240h**, **6-hour 246–384h**. One parquet per run (00z, 06z, 12z, 18z). No dependency on the parent project — **you can move this folder anywhere** and it will still work.

### Setup

```bash
cd gefs_ensemble_pull
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

If you get numpy/bottleneck errors, use: `pip install "numpy<2.3"`.

### Output

- **Directory:** `output/` (created inside this folder)
- **Files:** `gefs_temps_YYYYMMDD_HHz.parquet` (e.g. `gefs_temps_20260215_12z.parquet`)
- **Schema:** `vintage_time`, `forecast_hour`, `valid_time`, `city`, `member`, `temp_k`, `temp_f` (`member` = `"mean"`, `"0"`..`"30"`)
- **Rows per file:** 105 steps × 10 cities × 32 members = **33,600**

For degree days / load modeling: use 3h data through day 10, then linearly interpolate or hold values for the 6h segment when computing daily metrics.

### Run (from this folder or its parent)

From **inside** `gefs_ensemble_pull`:

```bash
# Date range (all 00z, 06z, 12z, 18z)
python run_fetch_all.py --start 2026-02-01 --end 2026-02-10

# Latest run only
python run_fetch_all.py --latest 1

# One run per day (00z only)
python run_fetch_all.py --start 2026-02-01 --end 2026-02-05 --hour 0
```

From the **parent** of this folder:

```bash
python -m gefs_ensemble_pull.run_fetch_all --start 2026-02-01 --end 2026-02-10
```

### Fill gaps (no delete, merge only)

```bash
python double_check.py --start 2026-02-17 --end 2026-02-18 --dry-run   # see what’s missing
python double_check.py --start 2026-02-17 --end 2026-02-18              # fill
```

### Moving the folder

All paths are relative to this folder (`config.py` uses `os.path.dirname(os.path.abspath(__file__))`). Copy or move `gefs_ensemble_pull` anywhere; run the same commands from inside it (or use `python -m gefs_ensemble_pull.run_fetch_all` with the folder on `PYTHONPATH`).

### Files

| File | Purpose |
|------|--------|
| `config.py` | Output dir, cities, bounds, GEFS 3h/6h steps, GEFS_MEMBERS (mean + 0..30) |
| `gefs_utils.py` | Latest/previous GEFS run time (for `--latest`) |
| `fetch_full_run.py` | Fetch one run: all 32 members, 0–384h, 10 cities |
| `run_fetch_all.py` | CLI: date range or latest N runs → `output/` |
| `double_check.py` | Find missing/incomplete runs, fill without deleting |
| `requirements.txt` | herbie-data, pandas, xarray, pyarrow, cfgrib |

