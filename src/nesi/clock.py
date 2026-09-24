"""Nigeria time helpers. West Africa Time is UTC+1 all year (no DST)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

WAT = timezone(timedelta(hours=1), "WAT")


def now_wat() -> datetime:
    """Current Nigeria time as a naive datetime, matching how the DB stores it."""
    return datetime.now(WAT).replace(tzinfo=None, microsecond=0)


def floor_hour(dt: datetime) -> datetime:
    return dt.replace(minute=0, second=0, microsecond=0)
