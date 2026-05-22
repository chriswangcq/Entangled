"""Command-line entry point for Entangled SQLite-to-Postgres migration."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sqlite3
import sys
from typing import Sequence

from .database import PostgresDatabase
from .migration import (
    MigrationReport,
    MigrationSafetyError,
    execute_migration_plan,
    plan_migration,
    redact_secret,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Migrate Entangled SQLite data to Postgres")
    parser.add_argument("--sqlite-path", required=True, help="Path to the Entangled SQLite source DB")
    parser.add_argument("--report", required=True, help="Path for the redacted JSON migration report")
    parser.add_argument("--target-label", required=True, help="Non-secret target label written to reports")
    dsn_group = parser.add_mutually_exclusive_group()
    dsn_group.add_argument("--postgres-dsn", default="", help="Postgres DSN for execution mode")
    dsn_group.add_argument("--postgres-dsn-file", default="", help="File containing Postgres DSN")
    parser.add_argument("--clean-target", action="store_true", help="Allow destructive target cleanup planning")
    parser.add_argument(
        "--target-confirmation",
        default="",
        help="Must exactly match --target-label when --clean-target is used",
    )
    parser.add_argument("--dry-run", action="store_true", help="Plan only; do not connect to Postgres")
    return parser


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not args.dry_run and not args.postgres_dsn and not args.postgres_dsn_file:
        parser.error("non-dry-run migration requires --postgres-dsn or --postgres-dsn-file")
    return args


def write_report(path: Path, report: MigrationReport) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(report.to_dict(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _safe_error(exc: BaseException) -> str:
    return redact_secret(str(exc))


def run(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    sqlite_path = Path(args.sqlite_path)
    report_path = Path(args.report)
    dsn_file = Path(args.postgres_dsn_file) if args.postgres_dsn_file else None

    try:
        plan = plan_migration(
            sqlite_path,
            clean_target=args.clean_target,
            target_confirmation=args.target_confirmation,
            expected_target_confirmation=args.target_label,
        )
        if args.dry_run:
            report = plan.to_report(connection_label=args.target_label)
            write_report(report_path, report)
            return 0

        target = PostgresDatabase(dsn=args.postgres_dsn, dsn_file=dsn_file)
        target.connect()
        try:
            report = execute_migration_plan(
                sqlite_path,
                target,
                plan=plan,
                connection_label=args.target_label,
            )
        finally:
            target.close()
        write_report(report_path, report)
        return 0
    except (MigrationSafetyError, sqlite3.Error, ValueError, RuntimeError) as exc:
        print(f"migration failed: {_safe_error(exc)}", file=sys.stderr)
        return 2


def main(argv: Sequence[str] | None = None) -> int:
    return run(argv)


if __name__ == "__main__":
    raise SystemExit(main())
