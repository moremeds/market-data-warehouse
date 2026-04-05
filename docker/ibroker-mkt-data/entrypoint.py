#!/usr/bin/env python3
"""MDW container entrypoint — scheduler, job runner, and R2 sync.

Usage:
    python entrypoint.py                    # Start scheduler (daily at configured time)
    python entrypoint.py --now              # Run once immediately, then exit
    python entrypoint.py --seed --preset presets/sp500.json  # Initial backfill + upload
"""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger("mdw.entrypoint")

APP_DIR = Path(__file__).resolve().parent
SCRIPTS_DIR = APP_DIR / "scripts"


def _python() -> str:
    return sys.executable


def _run_cmd(cmd: list[str], label: str) -> int:
    """Run a command, log output, return exit code."""
    logger.info("Running: %s", " ".join(cmd))
    result = subprocess.run(cmd, text=True, capture_output=False)
    if result.returncode != 0:
        logger.error("%s failed with exit code %d", label, result.returncode)
    else:
        logger.info("%s completed successfully", label)
    return result.returncode


def sync_download() -> int:
    """Download current bronze state from R2."""
    return _run_cmd([_python(), str(SCRIPTS_DIR / "sync_to_r2.py"), "--download"], "R2 download")


def sync_upload() -> int:
    """Upload bronze state to R2."""
    return _run_cmd([_python(), str(SCRIPTS_DIR / "sync_to_r2.py"), "--upload"], "R2 upload")


def run_daily_update(force: bool = False) -> int:
    """Run the full daily update (equity + futures + CBOE)."""
    cmd = [
        _python(), str(SCRIPTS_DIR / "run_daily_update_job.py"),
    ]
    if force:
        cmd.append("--force")
    return _run_cmd(cmd, "Daily update")


def run_seed(preset: str, years: int = 10) -> int:
    """Run initial backfill from a preset."""
    cmd = [
        _python(), str(SCRIPTS_DIR / "fetch_ib_historical.py"),
        "--preset", preset,
        "--years", str(years),
    ]
    return _run_cmd(cmd, f"Seed ({preset})")


def run_job_cycle(force: bool = False) -> int:
    """Full job cycle: download from R2 → daily update → upload to R2."""
    logger.info("=== Starting job cycle ===")

    # Step 1: Rehydrate local bronze from R2
    rc = sync_download()
    if rc != 0:
        logger.warning("R2 download failed (rc=%d), continuing with local state", rc)

    # Step 2: Run daily update (equity + futures + CBOE)
    rc = run_daily_update(force=force)

    # Step 3: Upload to R2 only on success
    if rc == 0:
        upload_rc = sync_upload()
        if upload_rc != 0:
            logger.error("R2 upload failed (rc=%d)", upload_rc)
            return upload_rc
    else:
        logger.warning("Daily update failed (rc=%d), skipping R2 upload", rc)

    logger.info("=== Job cycle complete (rc=%d) ===", rc)
    return rc


def next_run_time(hour: int, minute: int, tz: ZoneInfo) -> datetime:
    """Calculate the next occurrence of HH:MM in the given timezone."""
    now = datetime.now(tz)
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return target


def scheduler_loop(hour: int, minute: int, tz: ZoneInfo) -> None:
    """Run the job at the configured time, forever."""
    logger.info("Scheduler started: daily at %02d:%02d %s", hour, minute, tz)

    while True:
        target = next_run_time(hour, minute, tz)
        wait_seconds = (target - datetime.now(tz)).total_seconds()
        logger.info("Next run at %s (sleeping %.0f seconds)", target.isoformat(), wait_seconds)

        time.sleep(max(0, wait_seconds))
        run_job_cycle()


def main() -> int:
    parser = argparse.ArgumentParser(description="MDW container entrypoint")
    parser.add_argument("--now", action="store_true", help="Run once immediately, then exit")
    parser.add_argument("--force", action="store_true", help="Force run on non-trading days")
    parser.add_argument("--seed", action="store_true", help="Run initial backfill")
    parser.add_argument("--preset", type=str, help="Preset file for --seed (e.g. presets/sp500.json)")
    parser.add_argument("--years", type=int, default=10, help="Years of history for --seed (default: 10)")
    args = parser.parse_args()

    if args.seed:
        if not args.preset:
            logger.error("--seed requires --preset")
            return 1
        rc = run_seed(args.preset, args.years)
        if rc == 0:
            sync_upload()
        return rc

    if args.now:
        return run_job_cycle(force=args.force)

    # Default: scheduler loop
    hour = int(os.getenv("MDW_SCHEDULE_HOUR", "16"))
    minute = int(os.getenv("MDW_SCHEDULE_MINUTE", "5"))
    tz = ZoneInfo(os.getenv("MDW_SCHEDULE_TZ", "US/Eastern"))

    scheduler_loop(hour, minute, tz)
    return 0  # Unreachable, but satisfies type checker


if __name__ == "__main__":
    raise SystemExit(main())
