"""Print per-minute aggregates for one series from DynamoDB."""

from __future__ import annotations

import argparse
import os
from datetime import UTC, datetime, timedelta

from .store import AggregateStore


def main(argv: list[str] | None = None) -> int:
    import boto3

    p = argparse.ArgumentParser(prog="cloudstream-query")
    p.add_argument("source_id")
    p.add_argument("event_type")
    p.add_argument("--minutes", type=int, default=15)
    args = p.parse_args(argv)
    store = AggregateStore(
        boto3.client("dynamodb"),
        os.getenv("CLOUDSTREAM_AGG_TABLE", "cloudstream-aggregates"),
        os.getenv("CLOUDSTREAM_DEDUPE_TABLE", "cloudstream-dedupe"),
    )
    now = datetime.now(UTC)
    rows = store.series(
        f"{args.source_id}#{args.event_type}",
        (now - timedelta(minutes=args.minutes)).strftime("%Y-%m-%dT%H:%M"),
        now.strftime("%Y-%m-%dT%H:%M"),
    )
    for r in rows:
        print(f"{r['minute']}  count={r['count']:>5}  avg={r['avg']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
