"""Send email through the Resend HTTP API."""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request

from tenacity import before_sleep_log, retry, retry_if_exception, stop_after_attempt, wait_exponential

from nesi.config import Settings

log = logging.getLogger(__name__)

RESEND_URL = "https://api.resend.com/emails"


def _is_transient(e: BaseException) -> bool:
    if isinstance(e, urllib.error.HTTPError):
        return e.code == 429 or e.code >= 500
    return isinstance(e, (urllib.error.URLError, TimeoutError))


@retry(
    retry=retry_if_exception(_is_transient),
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=2, min=2, max=30),
    before_sleep=before_sleep_log(log, logging.WARNING),
    reraise=True,
)
def send(settings: Settings, subject: str, html: str, text: str, to: tuple[str, ...] = ()) -> None:
    """Email `to` (default: the report recipients)."""
    recipients = list(to or settings.report_to)
    body = json.dumps(
        {
            "from": settings.report_from,
            "to": recipients,
            "subject": subject,
            "html": html,
            "text": text,
        }
    ).encode()
    req = urllib.request.Request(
        RESEND_URL,
        data=body,
        method="POST",
        headers={
            "Authorization": f"Bearer {settings.resend_api_key}",
            "Content-Type": "application/json",
            "User-Agent": "nesi-hourly-update/2.0",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            email_id = json.load(resp).get("id")
    except urllib.error.HTTPError as e:
        log.error("Resend rejected the email (%s): %s", e.code, e.read().decode(errors="replace"))
        raise
    log.info("Sent email %r to %d recipient(s), id=%s", subject, len(recipients), email_id)
