from datetime import UTC, datetime

from nesi.scheduler import heartbeat_is_fresh, next_run, touch_heartbeat


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
