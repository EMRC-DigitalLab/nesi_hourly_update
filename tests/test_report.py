from datetime import date, datetime
from decimal import Decimal

from nesi.report import HourTotal, ReportData, render_html, render_text, report_day, subject, summarise


def _data(now: datetime, hours: list[str], jobs: dict | None = None) -> ReportData:
    return ReportData(
        now=now,
        day=report_day(now),
        hours=[HourTotal(h, 22, Decimal("4000.5")) for h in hours],
        top_gencos=[("EGBIN (STEAM)", Decimal("9000"))],
        disco_at=datetime(2026, 9, 24, 15, 5),
        disco=[("Abuja Disco", Decimal("700")), ("Eko Disco", Decimal("544"))],
        jobs=jobs if jobs is not None else {"genco": {"ok": True}, "disco": {"ok": True}},
    )


def test_report_day_at_midnight_is_yesterday():
    assert report_day(datetime(2026, 9, 25, 0, 5)) == date(2026, 9, 24)
    assert report_day(datetime(2026, 9, 24, 15, 5)) == date(2026, 9, 24)


def test_latest_hour_not_yet_published_is_not_missing():
    s = summarise(_data(datetime(2026, 9, 24, 15, 5), [f"{h:02d}:00" for h in range(1, 15)]))
    assert s.missing == []
    assert s.latest_hour == "15:00" and s.latest_pending
    assert s.healthy


def test_gaps_are_reported():
    hours = [f"{h:02d}:00" for h in range(1, 16) if h not in (3, 7)]
    s = summarise(_data(datetime(2026, 9, 24, 15, 5), hours))
    assert s.missing == ["03:00", "07:00"]
    assert not s.healthy
    assert s.total_mwh == Decimal("4000.5") * 13


def test_midnight_report_covers_full_previous_day():
    s = summarise(_data(datetime(2026, 9, 25, 0, 5), [f"{h:02d}:00" for h in range(1, 25)]))
    assert s.missing == [] and not s.latest_pending


def test_failing_job_drives_headline():
    data = _data(datetime(2026, 9, 24, 9, 5), ["01:00"], jobs={"genco": {"ok": False}, "disco": {"ok": True}})
    s = summarise(data)
    assert s.failing_jobs == ["genco"]
    assert "GENCO generation failing" in subject(data, s)


def test_render_contains_button_and_escapes():
    data = _data(datetime(2026, 9, 24, 9, 5), ["01:00"])
    data.top_gencos = [("<script>", Decimal(1))]
    s = summarise(data)
    body = render_html(data, s, "https://nesi.example/rerun?exp=1&sig=x")
    assert "Re-run now" in body and "&lt;script&gt;" in body and "<script>" not in body
    assert "https://nesi.example/rerun?exp=1&amp;sig=x" in body
    assert "Re-run now: https://nesi.example/rerun" in render_text(data, s, "https://nesi.example/rerun")
