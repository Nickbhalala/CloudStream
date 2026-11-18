"""Lambda handler for a Kinesis event source mapping with ReportBatchItemFailures.

Record outcomes:
- valid          -> applied to DynamoDB once (duplicates are detected and skipped)
- invalid data   -> sent to the dead-letter SQS queue with the reason; never retried
- transient error (throttling, timeouts) -> reported as a batch item failure. Lambda
  retries from that record; records before it are already committed, and the dedupe
  table makes re-applying any of them a no-op.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import time
from datetime import UTC, datetime
from functools import lru_cache
from typing import Any

import boto3

from .events import InvalidEvent, parse_event
from .metrics import Metrics
from .store import AggregateStore, WriteResult

log = logging.getLogger()
log.setLevel(logging.INFO)


@lru_cache(maxsize=1)
def _clients() -> tuple[AggregateStore, Any, str]:
    """Created once per Lambda container and reused across invocations."""
    store = AggregateStore(
        boto3.client("dynamodb"), os.environ["AGGREGATES_TABLE"], os.environ["DEDUPE_TABLE"]
    )
    return store, boto3.client("sqs"), os.environ["DLQ_URL"]


def process_batch(
    event: dict,
    store: AggregateStore,
    sqs: Any,
    dlq_url: str,
    metrics: Metrics,
    now: datetime | None = None,
) -> dict:
    records = event.get("Records", [])
    metrics.add("RecordsReceived", len(records))
    now = now or datetime.now(UTC)
    failures: list[dict[str, str]] = []
    max_age_ms = 0.0
    started = time.perf_counter()

    for i, rec in enumerate(records):
        kin = rec["kinesis"]
        seq = kin["sequenceNumber"]
        arrival = kin.get("approximateArrivalTimestamp")
        if arrival:
            max_age_ms = max(max_age_ms, now.timestamp() * 1000 - float(arrival) * 1000)
        try:
            payload = base64.b64decode(kin["data"])
            evt = parse_event(payload, now=now)
        except (InvalidEvent, ValueError) as e:
            _dead_letter(sqs, dlq_url, kin, str(e))
            metrics.add("InvalidRecords", 1)
            continue

        try:
            result = store.record(evt)
        except Exception:
            log.exception("transient failure on %s; retrying from this record", seq)
            failures.append({"itemIdentifier": seq})
            metrics.add("RetriedRecords", len(records) - i)
            break  # Lambda retries from the first failure, so stop here.
        metrics.add("DuplicateRecords" if result is WriteResult.DUPLICATE else "AppliedRecords", 1)

    metrics.put("BatchProcessingMs", (time.perf_counter() - started) * 1000, "Milliseconds")
    metrics.put("MaxRecordAgeMs", max_age_ms, "Milliseconds")
    return {"batchItemFailures": failures}


def _dead_letter(sqs: Any, url: str, kin: dict, reason: str) -> None:
    sqs.send_message(
        QueueUrl=url,
        MessageBody=json.dumps(
            {
                "reason": reason,
                "partitionKey": kin.get("partitionKey"),
                "sequenceNumber": kin.get("sequenceNumber"),
                "data": kin.get("data"),
            }
        ),
    )


def lambda_handler(event: dict, context: Any) -> dict:
    store, sqs, dlq_url = _clients()
    metrics = Metrics({"Function": getattr(context, "function_name", "local")})
    try:
        return process_batch(event, store, sqs, dlq_url, metrics)
    finally:
        metrics.flush()
