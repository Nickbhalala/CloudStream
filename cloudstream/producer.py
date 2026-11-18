"""Sends events to Kinesis with PutRecords, retrying only the entries that failed."""

from __future__ import annotations

import argparse
import os
import random
import time
import uuid
from collections.abc import Iterable, Iterator
from datetime import UTC, datetime
from typing import Any

from .events import Event

MAX_BATCH = 500  # PutRecords limit


def synthetic_events(n: int, sources: int = 20, seed: int | None = None) -> Iterator[Event]:
    rng = random.Random(seed)
    types = ["temperature", "humidity", "power_draw"]
    for _ in range(n):
        yield Event(
            event_id=str(uuid.UUID(int=rng.getrandbits(128))),
            source_id=f"sensor-{rng.randrange(sources):03d}",
            event_type=rng.choice(types),
            value=round(rng.uniform(0, 100), 2),
            ts=datetime.now(UTC),
        )


def _batches(items: Iterable[Any], size: int) -> Iterator[list[Any]]:
    batch: list[Any] = []
    for item in items:
        batch.append(item)
        if len(batch) == size:
            yield batch
            batch = []
    if batch:
        yield batch


def put_events(
    client: Any, stream: str, events: Iterable[Event], max_retries: int = 5, sleep=time.sleep
) -> dict[str, int]:
    """Returns {"sent": n, "failed": m}. Partition key = source_id keeps each sensor ordered."""
    sent = failed = 0
    for batch in _batches(events, MAX_BATCH):
        entries = [{"Data": e.to_json().encode(), "PartitionKey": e.source_id} for e in batch]
        for attempt in range(max_retries + 1):
            resp = client.put_records(StreamName=stream, Records=entries)
            if not resp.get("FailedRecordCount"):
                sent += len(entries)
                break
            retry = [
                entry for entry, r in zip(entries, resp["Records"], strict=True) if "ErrorCode" in r
            ]
            sent += len(entries) - len(retry)
            entries = retry
            if attempt < max_retries:
                sleep(min(2.0, 0.1 * 2**attempt))  # backoff on throttling
        else:
            failed += len(entries)
    return {"sent": sent, "failed": failed}


def main(argv: list[str] | None = None) -> int:
    import boto3

    p = argparse.ArgumentParser(prog="cloudstream-produce")
    p.add_argument("--stream", default=os.getenv("CLOUDSTREAM_STREAM", "cloudstream-events"))
    p.add_argument("--count", type=int, default=1000)
    p.add_argument("--sources", type=int, default=20)
    args = p.parse_args(argv)
    result = put_events(
        boto3.client("kinesis"), args.stream, synthetic_events(args.count, args.sources)
    )
    print(result)
    return 0 if result["failed"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
