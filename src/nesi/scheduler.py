"""Long-running hourly scheduler.

Each job runs in its own subprocess with a hard timeout, one after the other,
so only one Chromium is ever alive and a hung browser can always be killed.
"""

from __future__ import annotations

import logging
import os
import signal
import subprocess
import sys
import time
import urllib.request
from datetime import UTC, datetime, timedelta
from pathlib import Path

from nesi.config import Settings

log = logging.getLogger(__name__)

JOBS = ("genco", "disco")
HEARTBEAT_INTERVAL = 30  # seconds
HEARTBEAT_MAX_AGE = 180  # seconds; used by the container healthcheck

_stopping = False


def next_run(now: datetime, minute: int) -> datetime:
    candidate = now.replace(minute=minute, second=0, microsecond=0)
    return candidate if candidate > now else candidate + timedelta(hours=1)


def touch_heartbeat(path: str) -> None:
    Path(path).touch()


def heartbeat_is_fresh(path: str, max_age: int = HEARTBEAT_MAX_AGE) -> bool:
    try:
        return time.time() - Path(path).stat().st_mtime < max_age
    except FileNotFoundError:
        return False


def ping(url: str, suffix: str = "") -> None:
    """Best-effort healthchecks.io ping; never fails the job."""
    if not url:
        return
    try:
        urllib.request.urlopen(url.rstrip("/") + suffix, timeout=10).close()
    except Exception as e:
        log.warning("Healthcheck ping to %s%s failed: %s", url, suffix, e)


def run_job(job: str, settings: Settings) -> bool:
    ping_url = getattr(settings, f"healthcheck_url_{job}")
    ping(ping_url, "/start")
    started = time.monotonic()
    log.info("Starting job %s", job)

    # New session so a timeout kills Chromium's child processes too.
    proc = subprocess.Popen([sys.executable, "-m", "nesi", job], start_new_session=True)
    deadline = started + settings.job_timeout
    while proc.poll() is None:
        if time.monotonic() > deadline:
            log.error("Job %s exceeded %ds; killing it", job, settings.job_timeout)
            os.killpg(proc.pid, signal.SIGKILL)
            proc.wait()
            break
        touch_heartbeat(settings.heartbeat_file)
        time.sleep(5)

    ok = proc.returncode == 0
    elapsed = time.monotonic() - started
    if ok:
        log.info("Job %s succeeded in %.0fs", job, elapsed)
        ping(ping_url)
    else:
        log.error("Job %s failed (exit %s) after %.0fs", job, proc.returncode, elapsed)
        ping(ping_url, "/fail")
    return ok


def _request_stop(signum, _frame) -> None:
    global _stopping
    log.info("Received signal %d; stopping after the current step", signum)
    _stopping = True


def run_forever(settings: Settings) -> None:
    signal.signal(signal.SIGTERM, _request_stop)
    signal.signal(signal.SIGINT, _request_stop)
    log.info("Scheduler started; jobs %s run hourly at minute %02d UTC", ", ".join(JOBS), settings.run_minute)

    while not _stopping:
        target = next_run(datetime.now(UTC), settings.run_minute)
        log.info("Next run at %s", target.isoformat(timespec="minutes"))
        while not _stopping and datetime.now(UTC) < target:
            touch_heartbeat(settings.heartbeat_file)
            remaining = (target - datetime.now(UTC)).total_seconds()
            time.sleep(max(0.0, min(HEARTBEAT_INTERVAL, remaining)))
        for job in JOBS:
            if _stopping:
                break
            run_job(job, settings)
    log.info("Scheduler stopped")
