from datetime import date

from nesi.store import LOGIN_EMAIL_INTERVAL, LOGIN_TOKEN_TTL


def test_latest_reruns_are_deduplicated_while_queued(store):
    a = store.enqueue_latest("x")
    assert store.enqueue_latest("y").id == a.id
    store.mark_running(a.id)
    assert store.enqueue_latest("z").id != a.id


def test_running_run_is_resumed_before_queued_ones(store):
    first = store.enqueue_dates(date(2026, 9, 1), date(2026, 9, 3), "x")
    store.enqueue_latest("y")
    store.mark_running(first.id)
    assert store.next_active().id == first.id


def test_date_progress_and_failures(store):
    run = store.enqueue_dates(date(2026, 9, 1), date(2026, 9, 3), "x")
    assert run.days_total == 3 and run.next_day == date(2026, 9, 1)
    run = store.record_day(run.id, date(2026, 9, 1), ok=True)
    run = store.record_day(run.id, date(2026, 9, 2), ok=False)
    run = store.record_day(run.id, date(2026, 9, 3), ok=False)
    assert run.days_done == 3 and run.failed_days == ("2026-09-02", "2026-09-03")
    assert store.finish(run.id, ok=False).status == "failed"
    assert store.next_active() is None


def test_login_token_single_use_and_expiry(store):
    token = store.create_login_token("a@x", now=1000)
    assert store.consume_login_token(token, now=1001) == "a@x"
    assert store.consume_login_token(token, now=1002) is None  # already used

    late = store.create_login_token("b@x", now=1000)
    assert store.consume_login_token(late, now=1000 + LOGIN_TOKEN_TTL + 1) is None


def test_login_emails_are_rate_limited(store):
    assert store.create_login_token("a@x", now=1000)
    assert store.create_login_token("a@x", now=1010) is None
    assert store.create_login_token("a@x", now=1000 + LOGIN_EMAIL_INTERVAL + 1)
