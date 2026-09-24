"""Long-running scheduler: hourly scrapes, email reports and queued re-runs.

Each job runs in its own subprocess with a hard timeout, one after the other,
so only one Chromium is ever alive and a hung browser can always be killed.

Every hour: genco, disco, then an email report at the configured Nigeria hours.
Between hours it works through the re-run queue (web site, report email link,
GitHub workflow, `nesi rerun`). Date re-runs advance one day per step, so the
hourly scrape is never held up for long by a big backfill.
"""

from __future__ import annotations

import html
import json
import logging
import os
import signal
import subprocess
import sys
import time
import urllib.request
from datetime import UTC, datetime, timedelta
from pathlib import Path

from nesi import mailer
from nesi.clock import now_wat
from nesi.config import Settings
from nesi.store import Run, Store

log = logging.getLogger(__name__)

SCRAPE_JOBS = ("genco", "disco")
TICK = 5  # seconds between heartbeat / queue checks
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


def run_job(settings: Settings, job: str, *args: str) -> bool:
    ping_url = getattr(settings, f"healthcheck_url_{job}", "") if not args else ""
    ping(ping_url, "/start")
    started = time.monotonic()
    log.info("Starting job %s", " ".join((job, *args)))

    # New session so a timeout kills Chromium's child processes too.
    proc = subprocess.Popen([sys.executable, "-m", "nesi", job, *args], start_new_session=True)
    deadline = started + settings.job_timeout
    while proc.poll() is None:
        if time.monotonic() > deadline:
            log.error("Job %s exceeded %ds; killing it", job, settings.job_timeout)
            os.killpg(proc.pid, signal.SIGKILL)
            proc.wait()
            break
        touch_heartbeat(settings.heartbeat_file)
        time.sleep(TICK)

    ok = proc.returncode == 0
    elapsed = time.monotonic() - started
    if ok:
        log.info("Job %s succeeded in %.0fs", job, elapsed)
        ping(ping_url)
    else:
        log.error("Job %s failed (exit %s) after %.0fs", job, proc.returncode, elapsed)
        ping(ping_url, "/fail")
    return ok


def _record_status(settings: Settings, job: str, ok: bool) -> bool:
    """Save the job outcome for the report; return True if it just started failing."""
    path = Path(settings.status_file)
    try:
        status = json.loads(path.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        status = {}
    previous = status.get(job, {})
    now = now_wat().strftime("%Y-%m-%d %H:%M")
    status[job] = {
        "ok": ok,
        "finished_at": now,
        "last_success": now if ok else previous.get("last_success"),
    }
    path.write_text(json.dumps(status))
    return not ok and previous.get("ok", True)


def run_cycle(settings: Settings, rerun: bool = False) -> bool:
    """genco + disco (+ report when due); True if both scrapes succeeded."""
    all_ok, newly_failed = True, False
    for job in SCRAPE_JOBS:
        if _stopping:
            return False
        ok = run_job(settings, job)
        all_ok &= ok
        newly_failed |= _record_status(settings, job, ok)

    # Report on schedule, after every re-run, and as an alert the first time a job fails.
    scheduled = now_wat().hour in settings.report_hours
    if settings.reports_enabled and (scheduled or rerun or newly_failed) and not _stopping:
        run_job(settings, "report", *(["--rerun"] if rerun else []))
    return all_ok


def _notify_dates_done(settings: Settings, store: Store, run: Run) -> None:
    if not settings.reports_enabled:
        return
    from nesi.web import describe, status_text

    what = describe(run)
    outcome = status_text(run)
    to = (run.requested_by,) if store.has_user(run.requested_by) else ()
    if not (to or settings.report_to):
        return
    body = (
        '<div style="font-family:-apple-system,Segoe UI,Roboto,Arial,sans-serif;color:#1f2328">'
        f"<p>Re-run finished: <b>{html.escape(what)}</b></p><p>Result: {html.escape(outcome)}</p>"
        f'<p><a href="{html.escape(settings.public_base_url)}/">Open the re-run page</a></p></div>'
    )
    try:
        mailer.send(
            settings, f"Re-run {outcome.split(' ·')[0].lower()}: {what}", body, f"{what}\n{outcome}", to=to
        )
    except Exception:
        log.exception("Could not send re-run completion email")


def step(settings: Settings, store: Store, run: Run) -> None:
    """Do one unit of a queued re-run: a full latest cycle, or one day of a date range."""
    store.mark_running(run.id)
    if run.kind == "latest":
        store.finish(run.id, run_cycle(settings, rerun=True))
        return
    day = run.next_day
    ok = run_job(settings, "genco", "--from", day.isoformat())
    run = store.record_day(run.id, day, ok)
    if run.days_done >= run.days_total:
        run = store.finish(run.id, not run.failed_days)
        log.info("Re-run #%d finished: %s", run.id, run.status)
        _notify_dates_done(settings, store, run)


def _request_stop(signum, _frame) -> None:
    global _stopping
    log.info("Received signal %d; stopping after the current step", signum)
    _stopping = True


def run_forever(settings: Settings) -> None:
    signal.signal(signal.SIGTERM, _request_stop)
    signal.signal(signal.SIGINT, _request_stop)
    store = Store(settings.state_db)
    store.ensure_owners(settings.dashboard_users)
    log.info(
        "Scheduler started; %s run hourly at minute %02d UTC; reports at %s WAT (%s)",
        ", ".join(SCRAPE_JOBS),
        settings.run_minute,
        ", ".join(f"{h:02d}:00" for h in sorted(settings.report_hours)),
        "enabled" if settings.reports_enabled else "disabled",
    )
    if settings.web_enabled:
        from nesi.web import start_web_server

        start_web_server(settings, store)

    due = next_run(datetime.now(UTC), settings.run_minute)
    log.info("Next hourly run at %s", due.isoformat(timespec="minutes"))
    while not _stopping:
        touch_heartbeat(settings.heartbeat_file)
        if datetime.now(UTC) >= due:
            run_cycle(settings)
            due = next_run(datetime.now(UTC), settings.run_minute)
            log.info("Next hourly run at %s", due.isoformat(timespec="minutes"))
        elif run := store.next_active():
            step(settings, store, run)
        else:
            time.sleep(max(0.0, min(TICK, (due - datetime.now(UTC)).total_seconds())))
    log.info("Scheduler stopped")
