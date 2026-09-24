"""Runtime settings, read from environment variables (see .env.example)."""

from __future__ import annotations

import os
from dataclasses import dataclass


class ConfigError(RuntimeError):
    pass


def _required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise ConfigError(f"Missing required environment variable: {name}")
    return value


def _int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError as e:
        raise ConfigError(f"{name} must be an integer, got {raw!r}") from e


@dataclass(frozen=True)
class Settings:
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str

    niggrid_url: str
    # Minute past the hour (UTC) at which the scheduler fires.
    run_minute: int
    # Hard wall-clock limit for one job run, in seconds.
    job_timeout: int
    # Until this Nigeria-time hour, GENCO runs also re-scrape the previous day,
    # to pick up the 24:00 reading and late corrections.
    genco_revalidate_until_hour: int

    # Optional healthchecks.io (or compatible) ping URLs.
    healthcheck_url_genco: str
    healthcheck_url_disco: str

    heartbeat_file: str
    # Scheduler writes the latest outcome of each job here for the report.
    status_file: str
    # SQLite file (on a volume) holding the re-run queue/history and sign-in tokens.
    state_db: str

    # Email reports via Resend; disabled when the API key is empty.
    resend_api_key: str
    report_from: str
    report_to: tuple[str, ...]
    # Nigeria-time hours at which a report is emailed (after that hour's run).
    report_hours: frozenset[int]

    # Public HTTPS URL of the re-run site (nginx -> WEB_PORT). When empty,
    # the site is off and the email's Re-run button opens the GitHub workflow.
    public_base_url: str
    # Signs email re-run links, sign-in sessions and CSRF tokens.
    rerun_secret: str
    github_rerun_url: str
    web_port: int
    # Owner emails: always allowed to sign in, and can't be removed from the
    # site. Everyone else is added on the site by someone already signed in.
    dashboard_users: frozenset[str]

    @property
    def reports_enabled(self) -> bool:
        """Email is configured; recipients come from REPORT_TO and the site's people list."""
        return bool(self.resend_api_key and self.report_from)

    @property
    def web_enabled(self) -> bool:
        return bool(self.public_base_url and self.rerun_secret)

    @classmethod
    def from_env(cls) -> Settings:
        report_to = _csv("REPORT_TO")
        settings = cls(
            db_host=_required("DB_HOST"),
            db_port=_int("DB_PORT", 3306),
            db_name=_required("DB_NAME"),
            db_user=_required("DB_USER"),
            db_password=_required("DB_PASSWORD"),
            niggrid_url=os.environ.get("NIGGRID_URL", "https://www.niggrid.org").rstrip("/"),
            run_minute=_int("RUN_MINUTE", 5),
            job_timeout=_int("JOB_TIMEOUT_SECONDS", 900),
            genco_revalidate_until_hour=_int("GENCO_REVALIDATE_UNTIL_HOUR", 7),
            healthcheck_url_genco=os.environ.get("HEALTHCHECK_URL_GENCO", "").strip(),
            healthcheck_url_disco=os.environ.get("HEALTHCHECK_URL_DISCO", "").strip(),
            heartbeat_file=os.environ.get("HEARTBEAT_FILE", "/tmp/nesi-heartbeat"),
            status_file=os.environ.get("STATUS_FILE", "/tmp/nesi-status.json"),
            state_db=os.environ.get("STATE_DB", "/data/nesi.sqlite3"),
            resend_api_key=os.environ.get("RESEND_API_KEY", "").strip(),
            report_from=os.environ.get("REPORT_FROM", "").strip(),
            report_to=report_to,
            report_hours=frozenset(_int_csv("REPORT_HOURS", "0,9,12,15,18,21")),
            public_base_url=os.environ.get("PUBLIC_BASE_URL", "").strip().rstrip("/"),
            rerun_secret=os.environ.get("RERUN_SECRET", "").strip(),
            github_rerun_url=os.environ.get(
                "GITHUB_RERUN_URL",
                "https://github.com/EMRC-DigitalLab/nesi_hourly_update/actions/workflows/rerun.yml",
            ),
            web_port=_int("WEB_PORT", 8080),
            dashboard_users=frozenset(e.lower() for e in (_csv("DASHBOARD_USERS") or report_to)),
        )
        if not 0 <= settings.run_minute <= 59:
            raise ConfigError("RUN_MINUTE must be between 0 and 59")
        if not 0 <= settings.genco_revalidate_until_hour <= 24:
            raise ConfigError("GENCO_REVALIDATE_UNTIL_HOUR must be between 0 and 24")
        if any(not 0 <= h <= 23 for h in settings.report_hours):
            raise ConfigError("REPORT_HOURS must be hours between 0 and 23")
        return settings


def _csv(name: str, default: str = "") -> tuple[str, ...]:
    raw = os.environ.get(name, default)
    return tuple(part.strip() for part in raw.split(",") if part.strip())


def _int_csv(name: str, default: str) -> list[int]:
    try:
        return [int(part) for part in _csv(name, default)]
    except ValueError as e:
        raise ConfigError(f"{name} must be comma-separated integers") from e
