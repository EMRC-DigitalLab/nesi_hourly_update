import json
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from nesi import scheduler
from nesi.scheduler import _record_status, heartbeat_is_fresh, next_run, touch_heartbeat


def test_next_run_later_this_hour():
    now = datetime(2026, 9, 24, 14, 2, tzinfo=UTC)
    assert next_run(now, 5) == datetime(2026, 9, 24, 14, 5, tzinfo=UTC)


def test_next_run_rolls_to_next_hour_and_day():
    now = datetime(2026, 9, 24, 23, 5, tzinfo=UTC)
    assert next_run(now, 5) == datetime(2026, 9, 25, 0, 5, tzinfo=UTC)


def test_heartbeat(tmp_path):
    path = str(tmp_path / "hb")
    assert not heartbeat_is_fresh(path)
    touch_heartbeat(path)
    assert heartbeat_is_fresh(path)


def test_record_status_flags_only_new_failures(settings):
    assert _record_status(settings, "genco", ok=True) is False
    assert _record_status(settings, "genco", ok=False) is True
    assert _record_status(settings, "genco", ok=False) is False  # still failing: no new alert
    status = json.loads(Path(settings.status_file).read_text())["genco"]
    assert status["ok"] is False and status["last_success"]


@pytest.mark.parametrize(
    ("hour", "rerun", "fail", "reported"),
    [(9, False, False, True), (10, False, False, False), (10, True, False, True), (10, False, True, True)],
)
def test_run_cycle_report_triggers(settings, monkeypatch, hour, rerun, fail, reported):
    calls = []
    monkeypatch.setattr(scheduler, "run_job", lambda _s, job, *a: calls.append(job) or not fail)
    monkeypatch.setattr(scheduler, "now_wat", lambda: datetime(2026, 9, 24, hour, 5))
    assert scheduler.run_cycle(settings, rerun=rerun) is not fail
    assert calls == (["genco", "disco", "report"] if reported else ["genco", "disco"])


def test_date_rerun_advances_one_day_per_step(settings, store, monkeypatch):
    calls, sent = [], []
    monkeypatch.setattr(scheduler, "run_job", lambda _s, job, *a: calls.append(a) or a[1] != "2026-09-02")
    monkeypatch.setattr(scheduler.mailer, "send", lambda *a, **k: sent.append(k.get("to")))
    run = store.enqueue_dates(date(2026, 9, 1), date(2026, 9, 3), "ops@raven.example")

    for _ in range(3):
        scheduler.step(settings, store, store.next_active())

    assert calls == [("--from", "2026-09-01"), ("--from", "2026-09-02"), ("--from", "2026-09-03")]
    done = store.get(run.id)
    assert done.status == "failed" and done.failed_days == ("2026-09-02",)
    assert store.next_active() is None
    assert sent == [("ops@raven.example",)]
