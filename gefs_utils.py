"""
GEFS run-time helpers. Standalone — no project imports.
"""
from datetime import datetime, timedelta

GEFS_RUN_HOURS = [0, 6, 12, 18]


def get_latest_gefs_run(conservative: bool = True) -> datetime:
    """Latest GEFS run time (00/06/12/18 UTC). Conservative = 3h delay for availability."""
    now = datetime.utcnow()
    check_time = now - timedelta(hours=3) if conservative else now
    current_hour = check_time.hour
    latest_run_hour = max([h for h in GEFS_RUN_HOURS if h <= current_hour], default=18)
    if current_hour < GEFS_RUN_HOURS[0]:
        run_time = check_time.replace(hour=18, minute=0, second=0, microsecond=0) - timedelta(
            days=1
        )
    else:
        run_time = check_time.replace(hour=latest_run_hour, minute=0, second=0, microsecond=0)
    return run_time


def get_previous_gefs_run(current_run: datetime) -> datetime:
    """Previous GEFS run (6 hours before)."""
    return current_run - timedelta(hours=6)

