"""Shared headless Chromium setup."""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager

from playwright.sync_api import Page, sync_playwright

NAVIGATION_TIMEOUT_MS = 120_000


@contextmanager
def open_page() -> Generator[Page]:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        try:
            # niggrid.org has served an expired TLS certificate
            # (NET::ERR_CERT_DATE_INVALID); accept it like "Proceed anyway".
            context = browser.new_context(ignore_https_errors=True)
            page = context.new_page()
            page.set_default_timeout(NAVIGATION_TIMEOUT_MS)
            yield page
        finally:
            browser.close()
