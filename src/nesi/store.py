"""Local state in SQLite: the re-run queue/history and one-time sign-in tokens.

Lives on a Docker volume so history survives redeploys. Only the scheduler
container touches it (web thread, scheduler loop and `nesi rerun` CLI).
"""

from __future__ import annotations

import hashlib
import secrets
import sqlite3
import time
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

from nesi.clock import now_wat

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    kind         TEXT NOT NULL,              -- 'latest' or 'dates'
    start_date   TEXT,
    end_date     TEXT,
    requested_by TEXT NOT NULL,
    requested_at TEXT NOT NULL,
    status       TEXT NOT NULL,              -- queued, running, done, failed
    days_done    INTEGER NOT NULL DEFAULT 0,
    failed_days  TEXT NOT NULL DEFAULT '',   -- comma-separated ISO dates
    started_at   TEXT,
    finished_at  TEXT
);
CREATE TABLE IF NOT EXISTS users (
    email    TEXT PRIMARY KEY,
    reports  INTEGER NOT NULL DEFAULT 1,     -- also receives the report emails
    added_by TEXT NOT NULL,
    added_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS login_tokens (
    token_hash TEXT PRIMARY KEY,
    email      TEXT NOT NULL,
    created_at REAL NOT NULL,
    expires_at REAL NOT NULL,
    used       INTEGER NOT NULL DEFAULT 0
);
"""

LOGIN_TOKEN_TTL = 15 * 60  # seconds
LOGIN_EMAIL_INTERVAL = 60  # seconds between sign-in emails to one address


@dataclass(frozen=True)
class Run:
    id: int
    kind: str
    start_date: date | None
    end_date: date | None
    requested_by: str
    requested_at: str
    status: str
    days_done: int
    failed_days: tuple[str, ...]
    started_at: str | None
    finished_at: str | None

    @property
    def days_total(self) -> int:
        if self.kind != "dates":
            return 1
        return (self.end_date - self.start_date).days + 1

    @property
    def next_day(self) -> date:
        return self.start_date + timedelta(days=self.days_done)

    @property
    def active(self) -> bool:
        return self.status in ("queued", "running")

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> Run:
        return cls(
            id=row["id"],
            kind=row["kind"],
            start_date=date.fromisoformat(row["start_date"]) if row["start_date"] else None,
            end_date=date.fromisoformat(row["end_date"]) if row["end_date"] else None,
            requested_by=row["requested_by"],
            requested_at=row["requested_at"],
            status=row["status"],
            days_done=row["days_done"],
            failed_days=tuple(d for d in row["failed_days"].split(",") if d),
            started_at=row["started_at"],
            finished_at=row["finished_at"],
        )


@dataclass(frozen=True)
class User:
    email: str
    reports: bool
    added_by: str
    added_at: str


def _stamp() -> str:
    return now_wat().strftime("%Y-%m-%d %H:%M")


def _hash(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


class Store:
    def __init__(self, path: str):
        self.path = path
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with self._db() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript(SCHEMA)

    @contextmanager
    def _db(self) -> Generator[sqlite3.Connection]:
        conn = sqlite3.connect(self.path, timeout=10, isolation_level=None)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    # --- runs --------------------------------------------------------------

    def get(self, run_id: int) -> Run:
        with self._db() as db:
            return Run.from_row(db.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone())

    def enqueue_latest(self, requested_by: str) -> Run:
        """Queue a re-run of the latest data; reuses one that is already waiting."""
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM runs WHERE kind = 'latest' AND status = 'queued'").fetchone()
            if row is None:
                cur = db.execute(
                    "INSERT INTO runs (kind, requested_by, requested_at, status) "
                    "VALUES ('latest', ?, ?, 'queued')",
                    (requested_by, _stamp()),
                )
                row = db.execute("SELECT * FROM runs WHERE id = ?", (cur.lastrowid,)).fetchone()
            db.execute("COMMIT")
        return Run.from_row(row)

    def enqueue_dates(self, start: date, end: date, requested_by: str) -> Run:
        with self._db() as db:
            cur = db.execute(
                "INSERT INTO runs (kind, start_date, end_date, requested_by, requested_at, status) "
                "VALUES ('dates', ?, ?, ?, ?, 'queued')",
                (start.isoformat(), end.isoformat(), requested_by, _stamp()),
            )
        return self.get(cur.lastrowid)

    def next_active(self) -> Run | None:
        """The run to work on: a started one first (e.g. after a restart), else the oldest queued."""
        with self._db() as db:
            row = db.execute(
                "SELECT * FROM runs WHERE status IN ('running', 'queued') "
                "ORDER BY status = 'running' DESC, id LIMIT 1"
            ).fetchone()
        return Run.from_row(row) if row else None

    def mark_running(self, run_id: int) -> None:
        with self._db() as db:
            db.execute(
                "UPDATE runs SET status = 'running', started_at = COALESCE(started_at, ?) WHERE id = ?",
                (_stamp(), run_id),
            )

    def record_day(self, run_id: int, day: date, ok: bool) -> Run:
        with self._db() as db:
            db.execute(
                "UPDATE runs SET days_done = days_done + 1, failed_days = CASE WHEN ? THEN failed_days "
                "ELSE failed_days || CASE WHEN failed_days = '' THEN '' ELSE ',' END || ? END WHERE id = ?",
                (ok, day.isoformat(), run_id),
            )
        return self.get(run_id)

    def finish(self, run_id: int, ok: bool) -> Run:
        with self._db() as db:
            db.execute(
                "UPDATE runs SET status = ?, finished_at = ? WHERE id = ?",
                ("done" if ok else "failed", _stamp(), run_id),
            )
        return self.get(run_id)

    def recent(self, limit: int = 15) -> list[Run]:
        with self._db() as db:
            rows = db.execute("SELECT * FROM runs ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        return [Run.from_row(r) for r in rows]

    # --- people with access -----------------------------------------------

    def users(self) -> list[User]:
        with self._db() as db:
            rows = db.execute("SELECT * FROM users ORDER BY email").fetchall()
        return [User(r["email"], bool(r["reports"]), r["added_by"], r["added_at"]) for r in rows]

    def has_user(self, email: str) -> bool:
        with self._db() as db:
            return db.execute("SELECT 1 FROM users WHERE email = ?", (email,)).fetchone() is not None

    def add_user(self, email: str, added_by: str, reports: bool = True) -> bool:
        """Add someone; False if they already have access."""
        with self._db() as db:
            cur = db.execute(
                "INSERT OR IGNORE INTO users (email, reports, added_by, added_at) VALUES (?, ?, ?, ?)",
                (email, int(reports), added_by, _stamp()),
            )
        return cur.rowcount == 1

    def ensure_owners(self, owners: frozenset[str]) -> None:
        for email in owners:
            self.add_user(email, "setup")

    def remove_user(self, email: str) -> None:
        with self._db() as db:
            db.execute("DELETE FROM users WHERE email = ?", (email,))

    def set_reports(self, email: str, reports: bool) -> None:
        with self._db() as db:
            db.execute("UPDATE users SET reports = ? WHERE email = ?", (int(reports), email))

    def report_recipients(self) -> list[str]:
        with self._db() as db:
            return [
                r["email"] for r in db.execute("SELECT email FROM users WHERE reports = 1 ORDER BY email")
            ]

    # --- sign-in tokens ----------------------------------------------------

    def create_login_token(self, email: str, now: float | None = None) -> str | None:
        """New single-use token, or None if one was sent to this email very recently."""
        now = now or time.time()
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute("DELETE FROM login_tokens WHERE expires_at < ?", (now,))
            recent = db.execute(
                "SELECT 1 FROM login_tokens WHERE email = ? AND created_at > ?",
                (email, now - LOGIN_EMAIL_INTERVAL),
            ).fetchone()
            if recent:
                db.execute("COMMIT")
                return None
            token = secrets.token_urlsafe(32)
            db.execute(
                "INSERT INTO login_tokens (token_hash, email, created_at, expires_at) VALUES (?, ?, ?, ?)",
                (_hash(token), email, now, now + LOGIN_TOKEN_TTL),
            )
            db.execute("COMMIT")
        return token

    def consume_login_token(self, token: str, now: float | None = None) -> str | None:
        """Email for a valid unused token (marking it used), else None."""
        now = now or time.time()
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT email FROM login_tokens WHERE token_hash = ? AND used = 0 AND expires_at >= ?",
                (_hash(token), now),
            ).fetchone()
            if row:
                db.execute("UPDATE login_tokens SET used = 1 WHERE token_hash = ?", (_hash(token),))
            db.execute("COMMIT")
        return row["email"] if row else None
