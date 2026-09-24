"""MySQL access: connection, schema check and idempotent upserts."""

from __future__ import annotations

import logging
from collections.abc import Generator, Sequence
from contextlib import contextmanager

import pymysql
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from nesi.config import Settings

log = logging.getLogger(__name__)

GENCO_TABLE = "combined_hourly_energy_generated_mwh"
GENCO_UNIQUE_KEY = "uq_genco_date_hour_genco"
DISCO_TABLE = "discoloadprofile"
DISCO_UNIQUE_KEY = "uq_disco_hour_company"

# Both statements rely on the unique keys added by migrations/, which make
# a re-run overwrite the existing row instead of adding a duplicate.
GENCO_UPSERT = f"""
    INSERT INTO {GENCO_TABLE} (Date, Hour, Gencos, EnergyGeneratedMWh)
    VALUES (%s, %s, %s, %s) AS new
    ON DUPLICATE KEY UPDATE EnergyGeneratedMWh = new.EnergyGeneratedMWh
"""
DISCO_UPSERT = f"""
    INSERT INTO {DISCO_TABLE} (Date, Company, Load_Allocation_MW)
    VALUES (%s, %s, %s) AS new
    ON DUPLICATE KEY UPDATE Date = new.Date, Load_Allocation_MW = new.Load_Allocation_MW
"""

_db_retry = retry(
    retry=retry_if_exception_type(pymysql.err.OperationalError),
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=2, min=2, max=30),
    before_sleep=before_sleep_log(log, logging.WARNING),
    reraise=True,
)


class SchemaError(RuntimeError):
    pass


def _connect(settings: Settings) -> pymysql.connections.Connection:
    return pymysql.connect(
        host=settings.db_host,
        port=settings.db_port,
        user=settings.db_user,
        password=settings.db_password,
        database=settings.db_name,
        connect_timeout=10,
        read_timeout=120,
        write_timeout=120,
        autocommit=False,
    )


@contextmanager
def connection(settings: Settings) -> Generator[pymysql.connections.Connection]:
    conn = _connect(settings)
    try:
        yield conn
    finally:
        conn.close()


def check_schema(conn: pymysql.connections.Connection) -> None:
    """Fail fast if the unique keys the upserts depend on are missing."""
    with conn.cursor() as cur:
        for table, key in ((GENCO_TABLE, GENCO_UNIQUE_KEY), (DISCO_TABLE, DISCO_UNIQUE_KEY)):
            cur.execute(
                """
                SELECT COUNT(*) FROM information_schema.STATISTICS
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s
                  AND INDEX_NAME = %s AND NON_UNIQUE = 0
                """,
                (table, key),
            )
            (count,) = cur.fetchone()
            if not count:
                raise SchemaError(
                    f"Unique key {key} is missing on {table}. Run the SQL files in migrations/ first."
                )


@_db_retry
def upsert(settings: Settings, sql: str, rows: Sequence[tuple]) -> None:
    """Write all rows in one transaction; retried as a whole on connection errors."""
    with connection(settings) as conn:
        check_schema(conn)
        try:
            with conn.cursor() as cur:
                for row in rows:
                    cur.execute(sql, row)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    log.info("Upserted %d rows", len(rows))
