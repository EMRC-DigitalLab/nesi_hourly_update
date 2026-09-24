"""Command line entry point: python -m nesi <command>."""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date, timedelta

from nesi.config import Settings


def _date_range(start: date, end: date) -> list[date]:
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="nesi", description="NESI hourly scrapers")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("scheduler", help="hourly scrapes, reports, re-run queue and site (container default)")
    genco = sub.add_parser("genco", help="scrape GENCO readings once")
    genco.add_argument(
        "--from", dest="start", type=date.fromisoformat, help="backfill start day (YYYY-MM-DD)"
    )
    genco.add_argument(
        "--to", dest="end", type=date.fromisoformat, help="backfill end day (defaults to --from)"
    )
    sub.add_parser("disco", help="scrape the current DISCO load allocation once")
    report = sub.add_parser("report", help="email the report now")
    report.add_argument("--rerun", action="store_true", help="label it as a re-run report")
    sub.add_parser("rerun", help="ask the running scheduler to re-run all jobs now")
    sub.add_parser("check-db", help="verify DB connectivity, permissions and unique keys")
    sub.add_parser("healthcheck", help="exit 0 if the scheduler heartbeat is fresh")
    args = parser.parse_args(argv)
    if args.command == "genco" and args.end and (not args.start or args.end < args.start):
        parser.error("--to needs --from and must not be before it")

    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stdout,
    )
    log = logging.getLogger("nesi")

    # Imported lazily so `healthcheck` doesn't need DB settings or Playwright.
    if args.command == "healthcheck":
        from nesi.scheduler import heartbeat_is_fresh

        return 0 if heartbeat_is_fresh(os.environ.get("HEARTBEAT_FILE", "/tmp/nesi-heartbeat")) else 1

    settings = Settings.from_env()
    try:
        if args.command == "scheduler":
            from nesi.scheduler import run_forever

            run_forever(settings)
        elif args.command == "genco":
            from nesi import genco as genco_job

            days = _date_range(args.start, args.end or args.start) if args.start else None
            genco_job.run(settings, days)
        elif args.command == "disco":
            from nesi import disco

            disco.run(settings)
        elif args.command == "report":
            from nesi import report as report_job

            report_job.run(settings, rerun=args.rerun)
        elif args.command == "rerun":
            from nesi.store import Store

            run = Store(settings.state_db).enqueue_latest("command line")
            log.info("Re-run #%d queued", run.id)
        elif args.command == "check-db":
            from nesi import db

            with db.connection(settings) as conn:
                db.check_schema(conn)
            log.info("Database OK: connected and unique keys present")
    except Exception:
        log.exception("%s failed", args.command)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
