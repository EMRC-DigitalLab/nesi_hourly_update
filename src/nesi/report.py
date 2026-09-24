"""Scheduled email report: GENCO coverage and totals, DISCO allocation, job health."""

from __future__ import annotations

import html
import json
import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from nesi import db, mailer
from nesi.auth import rerun_link
from nesi.clock import floor_hour, now_wat
from nesi.config import Settings
from nesi.store import Store

log = logging.getLogger(__name__)

JOB_LABELS = {"genco": "GENCO generation", "disco": "DISCO load"}


@dataclass(frozen=True)
class HourTotal:
    hour: str
    gencos: int
    mwh: Decimal


@dataclass
class ReportData:
    now: datetime
    day: date
    hours: list[HourTotal]
    top_gencos: list[tuple[str, Decimal]]
    disco_at: datetime | None
    disco: list[tuple[str, Decimal]]
    jobs: dict[str, dict] = field(default_factory=dict)


@dataclass
class Summary:
    total_mwh: Decimal
    peak: HourTotal | None
    missing: list[str]
    # The newest hour may not be published yet; reported but not an error.
    latest_hour: str | None
    latest_pending: bool
    failing_jobs: list[str]

    @property
    def healthy(self) -> bool:
        return not self.missing and not self.failing_jobs


def report_day(now: datetime) -> date:
    """The day the latest completed hour belongs to (at 00:05 that's yesterday)."""
    return (floor_hour(now) - timedelta(hours=1)).date()


def summarise(data: ReportData) -> Summary:
    cutoff = floor_hour(data.now)
    start = datetime.combine(data.day, datetime.min.time())
    present = {h.hour for h in data.hours}
    expected, latest = [], None
    for n in range(1, 25):
        label = f"{n:02d}:00"
        t = start + timedelta(hours=n)
        if t < cutoff:
            expected.append(label)
        elif t == cutoff:
            latest = label
    return Summary(
        total_mwh=sum((h.mwh for h in data.hours), Decimal(0)),
        peak=max(data.hours, key=lambda h: h.mwh, default=None),
        missing=[h for h in expected if h not in present],
        latest_hour=latest,
        latest_pending=latest is not None and latest not in present,
        failing_jobs=[job for job, s in data.jobs.items() if not s.get("ok", True)],
    )


def load_job_status(path: str) -> dict[str, dict]:
    try:
        return json.loads(Path(path).read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def gather(settings: Settings, now: datetime) -> ReportData:
    day = report_day(now)
    with db.connection(settings) as conn, conn.cursor() as cur:
        cur.execute(
            f"SELECT Hour, COUNT(*), SUM(EnergyGeneratedMWh) FROM {db.GENCO_TABLE} "
            "WHERE Date = %s GROUP BY Hour ORDER BY Hour",
            (day,),
        )
        hours = [HourTotal(h, n, Decimal(s)) for h, n, s in cur.fetchall()]
        cur.execute(
            f"SELECT Gencos, SUM(EnergyGeneratedMWh) AS total FROM {db.GENCO_TABLE} "
            "WHERE Date = %s GROUP BY Gencos ORDER BY total DESC LIMIT 5",
            (day,),
        )
        top = [(g, Decimal(t)) for g, t in cur.fetchall()]
        cur.execute(
            f"SELECT Company, Load_Allocation_MW, Date FROM {db.DISCO_TABLE} "
            f"WHERE HourStart = (SELECT MAX(HourStart) FROM {db.DISCO_TABLE}) ORDER BY Company"
        )
        disco_rows = cur.fetchall()
    return ReportData(
        now=now,
        day=day,
        hours=hours,
        top_gencos=top,
        disco_at=max((r[2] for r in disco_rows), default=None),
        disco=[(c, Decimal(mw)) for c, mw, _ in disco_rows],
        jobs=load_job_status(settings.status_file),
    )


# --- rendering -------------------------------------------------------------

GREEN, AMBER, RED, INK, MUTED, LINE = "#0b6e4f", "#b25e09", "#b42318", "#1f2933", "#616e7c", "#e4e7eb"


def _n(value: Decimal) -> str:
    return f"{value:,.1f}"


def _headline(s: Summary) -> tuple[str, str]:
    if s.failing_jobs:
        names = " and ".join(JOB_LABELS.get(j, j) for j in s.failing_jobs)
        return RED, f"âŒ {names} failing"
    if s.missing:
        n = len(s.missing)
        return AMBER, f"âš ï¸ {n} hour{'s' if n > 1 else ''} missing"
    return GREEN, "âœ… All good"


def subject(data: ReportData, s: Summary, rerun: bool = False) -> str:
    kind = "Re-run report" if rerun else "NESI report"
    return f"{kind} Â· {data.now:%a %d %b, %H:%M} WAT Â· {_headline(s)[1]}"


def _table(rows: list[tuple[str, ...]], header: tuple[str, ...], align_right_from: int = 1) -> str:
    th = "".join(
        f'<th style="text-align:{"right" if i >= align_right_from else "left"};padding:6px 8px;'
        f'border-bottom:2px solid {LINE};color:{MUTED};font-size:12px;font-weight:600">{html.escape(h)}</th>'
        for i, h in enumerate(header)
    )
    body = "".join(
        "<tr>"
        + "".join(
            f'<td style="text-align:{"right" if i >= align_right_from else "left"};padding:6px 8px;'
            f'border-bottom:1px solid {LINE};font-size:13px">{html.escape(c)}</td>'
            for i, c in enumerate(row)
        )
        + "</tr>"
        for row in rows
    )
    return f'<table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse">{th}{body}</table>'


def _section(title: str, content: str) -> str:
    return f'<h2 style="font-size:15px;margin:28px 0 10px;color:{INK}">{html.escape(title)}</h2>{content}'


def render_html(data: ReportData, s: Summary, link: str) -> str:
    color, headline = _headline(s)
    stats = [
        ("Energy so far", f"{_n(s.total_mwh)} MWh"),
        ("Peak hour", f"{s.peak.hour} Â· {_n(s.peak.mwh)} MWh" if s.peak else "â€”"),
        ("Hours in", f"{len(data.hours)}"),
    ]
    stat_cells = "".join(
        f'<td style="padding:12px;background:#f7f8fa;border-radius:6px" width="33%">'
        f'<div style="font-size:12px;color:{MUTED}">{k}</div>'
        f'<div style="font-size:17px;font-weight:600;margin-top:4px">{html.escape(v)}</div></td>'
        for k, v in stats
    )
    notes = []
    if s.missing:
        notes.append(f"Missing hours: <b>{', '.join(s.missing)}</b>")
    if s.latest_pending:
        notes.append(f"{s.latest_hour} not published by niggrid.org yet (normal for the latest hour).")
    notes_html = "".join(f'<p style="margin:6px 0;font-size:13px;color:{MUTED}">{n}</p>' for n in notes)

    hour_rows = [(h.hour, str(h.gencos), _n(h.mwh)) for h in data.hours]
    top_rows = [(g, _n(t)) for g, t in data.top_gencos]
    disco_rows = [(c, _n(mw)) for c, mw in data.disco]
    if data.disco:
        disco_rows.append(("Total", _n(sum((mw for _, mw in data.disco), Decimal(0)))))
    disco_title = f"DISCO load allocation Â· {data.disco_at:%d %b %H:%M}" if data.disco_at else "DISCO load"

    job_rows = []
    for job, label in JOB_LABELS.items():
        st = data.jobs.get(job)
        if not st:
            job_rows.append((label, "No run recorded yet", "â€”"))
            continue
        state = "OK" if st.get("ok") else "FAILED"
        job_rows.append((label, state, st.get("last_success") or "never"))

    return f"""<!doctype html><html><body style="margin:0;background:#f4f5f7;font-family:-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;color:{INK}">
<table width="100%" cellpadding="0" cellspacing="0"><tr><td align="center" style="padding:24px 12px">
<table width="600" cellpadding="0" cellspacing="0" style="max-width:600px;width:100%;background:#fff;border-radius:10px;padding:28px">
<tr><td>
<div style="font-size:12px;color:{MUTED};letter-spacing:.04em;text-transform:uppercase">NESI hourly update</div>
<h1 style="font-size:22px;margin:6px 0 4px">Generation report Â· {data.day:%a %d %b %Y}</h1>
<div style="font-size:13px;color:{MUTED}">As of {data.now:%H:%M} WAT</div>
<div style="margin:18px 0 8px;padding:12px 14px;border-left:4px solid {color};background:#f7f8fa;font-size:15px;font-weight:600">{headline}</div>
{notes_html}
<table width="100%" cellpadding="0" cellspacing="6" style="margin-top:12px"><tr>{stat_cells}</tr></table>
<div style="text-align:center;margin:24px 0 4px">
  <a href="{html.escape(link)}" style="display:inline-block;background:{GREEN};color:#fff;text-decoration:none;padding:12px 26px;border-radius:6px;font-weight:600">Re-run now</a>
  <div style="font-size:12px;color:{MUTED};margin-top:8px">Pulls the latest data from niggrid.org again and emails a fresh report.</div>
</div>
{_section("Scraper status", _table(job_rows, ("Job", "Last run", "Last success"), align_right_from=3))}
{_section("Top GENCOs (MWh)", _table(top_rows, ("Genco", "MWh")) if top_rows else "<p>No data.</p>")}
{_section(disco_title + " (MW)", _table(disco_rows, ("Company", "MW")) if disco_rows else "<p>No data.</p>")}
{_section("Hourly generation", _table(hour_rows, ("Hour", "Gencos", "MWh")) if hour_rows else "<p>No data.</p>")}
</td></tr></table>
</td></tr></table></body></html>"""


def render_text(data: ReportData, s: Summary, link: str) -> str:
    lines = [
        f"NESI generation report Â· {data.day:%a %d %b %Y} Â· as of {data.now:%H:%M} WAT",
        _headline(s)[1],
        "",
        f"Energy so far: {_n(s.total_mwh)} MWh",
        f"Peak hour: {s.peak.hour} ({_n(s.peak.mwh)} MWh)" if s.peak else "Peak hour: -",
        f"Hours in: {len(data.hours)}",
    ]
    if s.missing:
        lines.append(f"Missing hours: {', '.join(s.missing)}")
    for job, label in JOB_LABELS.items():
        st = data.jobs.get(job, {})
        lines.append(f"{label}: {'OK' if st.get('ok') else 'FAILED' if st else 'no run yet'}")
    lines += ["", f"Re-run now: {link}"]
    return "\n".join(lines)


def recipients(settings: Settings) -> tuple[str, ...]:
    """REPORT_TO plus everyone on the site's people list with reports switched on."""
    store = Store(settings.state_db)
    store.ensure_owners(settings.dashboard_users)
    return tuple(sorted({e.lower() for e in settings.report_to} | set(store.report_recipients())))


def run(settings: Settings, rerun: bool = False) -> None:
    if not settings.reports_enabled:
        log.info("Email reports disabled (set RESEND_API_KEY and REPORT_FROM)")
        return
    to = recipients(settings)
    if not to:
        log.info("No report recipients; add people on the re-run site or set REPORT_TO")
        return
    data = gather(settings, now_wat())
    s = summarise(data)
    link = rerun_link(settings)
    mailer.send(
        settings, subject(data, s, rerun), render_html(data, s, link), render_text(data, s, link), to=to
    )
