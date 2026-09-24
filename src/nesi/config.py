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

    @classmethod
    def from_env(cls) -> Settings:
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
        )
        if not 0 <= settings.run_minute <= 59:
            raise ConfigError("RUN_MINUTE must be between 0 and 59")
        if not 0 <= settings.genco_revalidate_until_hour <= 24:
            raise ConfigError("GENCO_REVALIDATE_UNTIL_HOUR must be between 0 and 24")
        return settings
