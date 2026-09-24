"""DISCO live load allocation (niggrid.org/discoloadprofile).

The page only shows the current allocation, so each run stores one row per
company for the current hour. Past hours cannot be re-scraped.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation

from playwright.sync_api import Page
from tenacity import before_sleep_log, retry, stop_after_attempt, wait_exponential

from nesi import db
from nesi.browser import open_page
from nesi.clock import now_wat
from nesi.config import Settings

log = logging.getLogger(__name__)

_HEADER_COMPANIES = {"company", "distribution company"}


class ScrapeError(RuntimeError):
    pass


@dataclass(frozen=True)
class Allocation:
    at: datetime
    company: str
    mw: Decimal

    def as_row(self) -> tuple:
        return (self.at, self.company, self.mw)


def parse_allocations(rows: list[list[str]], at: datetime) -> list[Allocation]:
    """Keep rows whose first cell names a Disco and whose second is a number."""
    allocations = []
    for cells in rows:
        if len(cells) < 2:
            continue
        company, load = cells[0].strip(), cells[1].strip()
        if "disco" not in company.lower() or company.lower() in _HEADER_COMPANIES:
            continue
        cleaned = load.replace(",", "").replace("MW", "").replace("(", "").replace(")", "").strip()
        try:
            mw = Decimal(cleaned)
        except InvalidOperation:
            log.warning("Could not parse load %r for %s", load, company)
            continue
        allocations.append(Allocation(at, company, mw))
    return allocations


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=10, min=10, max=60),
    before_sleep=before_sleep_log(log, logging.WARNING),
    reraise=True,
)
def scrape(page: Page, base_url: str) -> list[list[str]]:
    log.info("Scraping DISCO load profile")
    page.goto(f"{base_url}/discoloadprofile", wait_until="domcontentloaded")
    page.wait_for_load_state("networkidle")
    page.wait_for_timeout(5000)
    rows = page.eval_on_selector_all(
        "tr",
        "rows => rows.map(r => Array.from(r.querySelectorAll('td, th')).map(c => c.textContent.trim()))",
    )
    if not rows:
        raise ScrapeError("No table rows found on DISCO load profile page")
    return rows


def run(settings: Settings) -> None:
    with open_page() as page:
        rows = scrape(page, settings.niggrid_url)
    allocations = parse_allocations(rows, now_wat())
    if not allocations:
        raise ScrapeError("No DISCO allocations parsed from the page")
    log.info("Parsed %d DISCO allocations", len(allocations))
    db.upsert(settings, db.DISCO_UPSERT, [a.as_row() for a in allocations])
