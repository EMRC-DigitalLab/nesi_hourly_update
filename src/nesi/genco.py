"""GENCO hourly generation readings (niggrid.org "Genco Generation Readings")."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation

from playwright.sync_api import Page
from tenacity import before_sleep_log, retry, stop_after_attempt, wait_exponential

from nesi import db
from nesi.browser import open_page
from nesi.clock import floor_hour, now_wat
from nesi.config import Settings

log = logging.getLogger(__name__)

EXCLUDED_GENCOS = {"zTOTAL"}
_HOUR_LABEL = re.compile(r"^(\d{1,2}):(\d{2})$")
# The reading-date textbox shows e.g. "2026/09/24"; the others are fallbacks
# in case the site changes format.
_DATE_FORMATS = ("%Y/%m/%d", "%Y-%m-%d", "%d/%m/%Y")


class ScrapeError(RuntimeError):
    pass


@dataclass(frozen=True)
class Reading:
    day: date
    hour: str  # "01:00" .. "24:00", as shown on the site
    genco: str
    mwh: Decimal

    def as_row(self) -> tuple:
        return (self.day, self.hour, self.genco, self.mwh)


def hour_label_time(day: date, label: str) -> datetime | None:
    """Time an hour column refers to; "24:00" is midnight at the end of `day`."""
    m = _HOUR_LABEL.match(label.strip())
    if not m:
        return None
    return datetime.combine(day, datetime.min.time()) + timedelta(
        hours=int(m.group(1)), minutes=int(m.group(2))
    )


def _to_decimal(text: str) -> Decimal | None:
    text = text.replace(",", "").strip()
    if not text:
        return None
    try:
        value = Decimal(text)
    except InvalidOperation:
        return None
    return value if value.is_finite() else None


def parse_readings(
    headers: list[str], rows: list[list[str]], day: date, not_after: datetime | None = None
) -> list[Reading]:
    """Turn the scraped table into one Reading per (hour, genco).

    Hours later than `not_after` are skipped, so hours that haven't happened
    yet are never written.
    """
    if "Genco" not in headers:
        raise ScrapeError(f"No 'Genco' column in table headers: {headers}")
    genco_idx = headers.index("Genco")
    hour_cols = [
        (i, h)
        for i, h in enumerate(headers)
        if (t := hour_label_time(day, h)) is not None and (not_after is None or t <= not_after)
    ]

    readings = []
    skipped = 0
    for cells in rows:
        if len(cells) != len(headers):
            skipped += 1
            continue
        genco = cells[genco_idx].strip()
        if not genco or genco in EXCLUDED_GENCOS:
            continue
        for i, hour in hour_cols:
            mwh = _to_decimal(cells[i])
            if mwh is not None:
                readings.append(Reading(day, hour, genco, mwh))
    if skipped:
        log.debug("Skipped %d rows whose cell count did not match the headers", skipped)
    return readings


def _check_selected_date(page: Page, day: date) -> None:
    value = page.input_value("#MainContent_txtReadingDate").strip()
    for fmt in _DATE_FORMATS:
        try:
            shown = datetime.strptime(value, fmt).date()
        except ValueError:
            continue
        if shown != day:
            raise ScrapeError(f"Site shows readings for {shown}, expected {day}")
        return
    log.warning("Could not parse reading date %r; unable to confirm it is %s", value, day)


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=10, min=10, max=60),
    before_sleep=before_sleep_log(log, logging.WARNING),
    reraise=True,
)
def scrape_day(page: Page, base_url: str, day: date) -> tuple[list[str], list[list[str]]]:
    log.info("Scraping GENCO readings for %s", day)
    page.goto(f"{base_url}/", wait_until="domcontentloaded")
    page.wait_for_load_state("networkidle")
    page.locator("a[href*='/Analytics/GENCOGenerationPerformances']").first.click()
    page.wait_for_load_state("networkidle")
    page.locator("a[href*='/GenerationProfile2']").first.click()
    page.wait_for_load_state("networkidle")

    page.locator("#MainContent_txtReadingDate").click()
    page.locator("xpath=//*[@id='ui-datepicker-div']/div/div/select[2]").select_option(str(day.year))
    # jQuery UI months are zero-indexed.
    page.locator("xpath=//*[@id='ui-datepicker-div']/div/div/select[1]").select_option(str(day.month - 1))
    page.locator(f"xpath=//*[@id='ui-datepicker-div']/table/tbody/tr/td/a[text()='{day.day}']").click()

    page.locator("#MainContent_btnGetReadings").click()
    page.wait_for_load_state("networkidle")
    page.wait_for_timeout(8000)
    _check_selected_date(page, day)

    headers = page.eval_on_selector_all("th", "els => els.map(e => e.textContent.trim())")
    rows = page.eval_on_selector_all(
        "tr",
        """rows => rows
            .map(r => Array.from(r.querySelectorAll('td')).map(td => td.textContent.trim()))
            .filter(cells => cells.length > 0)""",
    )
    if headers and headers[0] == "":
        headers[0] = "Index"
    if not rows:
        raise ScrapeError(f"No table rows found for {day}")
    return headers, rows


def days_to_scrape(now: datetime, revalidate_until_hour: int) -> list[date]:
    """Today, plus yesterday in the early hours to catch 24:00 and late fixes."""
    today = now.date()
    if now.hour < revalidate_until_hour:
        return [today - timedelta(days=1), today]
    return [today]


def run(settings: Settings, days: list[date] | None = None) -> None:
    now = now_wat()
    days = days or days_to_scrape(now, settings.genco_revalidate_until_hour)
    cutoff = floor_hour(now)

    readings: list[Reading] = []
    with open_page() as page:
        for day in days:
            headers, rows = scrape_day(page, settings.niggrid_url, day)
            day_readings = parse_readings(headers, rows, day, not_after=cutoff)
            log.info("%s: %d readings", day, len(day_readings))
            readings.extend(day_readings)

    if not readings:
        raise ScrapeError(f"No GENCO readings found for {', '.join(map(str, days))}")
    db.upsert(settings, db.GENCO_UPSERT, [r.as_row() for r in readings])
