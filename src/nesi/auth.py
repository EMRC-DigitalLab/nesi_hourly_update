"""HMAC signing for email re-run links, sign-in sessions and CSRF tokens.

Everything is signed with RERUN_SECRET; rotating it invalidates all links and
sessions at once.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import time
from urllib.parse import urlencode

from nesi.config import Settings

LINK_TTL = 24 * 3600  # email re-run link
SESSION_TTL = 7 * 24 * 3600  # sign-in session


def _mac(secret: str, message: str) -> str:
    return hmac.new(secret.encode(), message.encode(), hashlib.sha256).hexdigest()


def _now(now: float | None) -> float:
    return time.time() if now is None else now


# --- email re-run links ------------------------------------------------------


def rerun_link(settings: Settings, now: float | None = None) -> str:
    """URL for the email's Re-run button."""
    if not settings.web_enabled:
        return settings.github_rerun_url
    exp = int(_now(now) + LINK_TTL)
    query = urlencode({"exp": exp, "sig": _mac(settings.rerun_secret, f"rerun:{exp}")})
    return f"{settings.public_base_url}/rerun?{query}"


def verify_link(secret: str, exp: str, sig: str, now: float | None = None) -> bool:
    try:
        exp_int = int(exp)
    except ValueError:
        return False
    if exp_int < _now(now):
        return False
    return hmac.compare_digest(_mac(secret, f"rerun:{exp_int}"), sig)


# --- sessions ----------------------------------------------------------------


def make_session(secret: str, email: str, now: float | None = None) -> str:
    exp = int(_now(now) + SESSION_TTL)
    who = base64.urlsafe_b64encode(email.encode()).decode().rstrip("=")
    return f"{who}.{exp}.{_mac(secret, f'session:{who}.{exp}')}"


def read_session(secret: str, value: str, now: float | None = None) -> str | None:
    """Signed-in email, or None if the cookie is missing, forged or expired."""
    try:
        who, exp, sig = value.split(".")
        if int(exp) < _now(now):
            return None
    except ValueError:
        return None
    if not hmac.compare_digest(_mac(secret, f"session:{who}.{exp}"), sig):
        return None
    return base64.urlsafe_b64decode(who + "=" * (-len(who) % 4)).decode()


def csrf_token(secret: str, session: str) -> str:
    return _mac(secret, f"csrf:{session}")[:32]
