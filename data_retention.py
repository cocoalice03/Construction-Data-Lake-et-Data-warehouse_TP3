"""Minimal data retention helper for the student data lake setup.

- Streams: keep only the last N days (folders year=YYYY/month=MM/day=DD).
- Tables: keep only the most recent versions (folders version=v1, v2, ...).

Example usage:
    python data_retention.py --stream transaction_stream --retention-days 30
    python data_retention.py --table user_transaction_summary --keep-versions 5
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta
import shutil
from pathlib import Path
from typing import Iterable

from data_lake_config import STREAMS_DIR, TABLES_DIR


def iter_partitions(base: Path) -> Iterable[Path]:
    """Yield all day-level partitions under base."""
    for year_dir in base.glob("year=*"):
        for month_dir in year_dir.glob("month=*"):
            for day_dir in month_dir.glob("day=*"):
                yield day_dir


def parse_partition_date(partition_dir: Path) -> datetime:
    """Extract a datetime from year=YYYY/month=MM/day=DD."""
    year = int(partition_dir.parent.parent.name.split("=")[1])
    month = int(partition_dir.parent.name.split("=")[1])
    day = int(partition_dir.name.split("=")[1])
    return datetime(year, month, day)


def cleanup_stream(stream_name: str, retention_days: int, dry_run: bool) -> None:
    base = STREAMS_DIR / stream_name
    if not base.exists():
        print(f"[WARN] Stream folder not found: {base}")
        return

    cutoff = datetime.now() - timedelta(days=retention_days)
    print(f"Cleaning stream '{stream_name}' before {cutoff.date()} (dry_run={dry_run})")

    for partition in iter_partitions(base):
        partition_date = parse_partition_date(partition)
        if partition_date < cutoff:
            if dry_run:
                print(f"  [dry-run] would remove {partition}")
            else:
                for file in partition.iterdir():
                    file.unlink()
                partition.rmdir()
                # remove empty parent directories if needed
                for parent in [partition.parent, partition.parent.parent]:
                    if parent.exists() and not any(parent.iterdir()):
                        parent.rmdir()
                print(f"  removed {partition}")


def cleanup_table(table_name: str, keep_versions: int, dry_run: bool) -> None:
    base = TABLES_DIR / table_name
    if not base.exists():
        print(f"[WARN] Table folder not found: {base}")
        return

    versions = sorted(
        [p for p in base.glob("version=v*") if p.is_dir()],
        key=lambda p: int(p.name.replace("version=v", "")),
    )

    if len(versions) <= keep_versions:
        print(f"Table '{table_name}': nothing to delete, {len(versions)} version(s).")
        return

    to_delete = versions[:-keep_versions]
    print(f"Cleaning table '{table_name}', removing {len(to_delete)} old version(s) (dry_run={dry_run})")

    for version_dir in to_delete:
        if dry_run:
            print(f"  [dry-run] would remove {version_dir}")
            continue

        shutil.rmtree(version_dir)
        print(f"  removed {version_dir}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Simple retention cleaner for the student data lake.")
    parser.add_argument("--stream", help="Name of the stream to clean")
    parser.add_argument("--table", help="Name of the table to clean")
    parser.add_argument("--retention-days", type=int, default=30, help="How many days to keep for streams")
    parser.add_argument("--keep-versions", type=int, default=5, help="How many versions to keep for tables")
    parser.add_argument("--dry-run", action="store_true", help="Only show what would be deleted")
    args = parser.parse_args()

    if not args.stream and not args.table:
        parser.error("Specify --stream and/or --table")

    if args.stream:
        cleanup_stream(args.stream, args.retention_days, args.dry_run)

    if args.table:
        cleanup_table(args.table, args.keep_versions, args.dry_run)


if __name__ == "__main__":
    main()
