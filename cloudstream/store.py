"""DynamoDB writes with exactly-once semantics per event_id.

Each event is one DynamoDB transaction:
  1. Put   dedupe[event_id]                 if attribute_not_exists(event_id)
  2. Update aggregates[series, minute]      ADD event_count 1, value_sum <value>

If the event was already applied (a Kinesis retry or a duplicate send), the condition
fails, the whole transaction is cancelled, and the aggregate is not double counted.
"""

from __future__ import annotations

import time
from decimal import Decimal
from enum import StrEnum
from typing import Any

from botocore.exceptions import ClientError

from .events import Event

DEDUPE_TTL_SECONDS = 7 * 24 * 3600  # matches the max accepted event age
_AGG_UPDATE = "ADD event_count :one, value_sum :v SET updated_at = :now"


class WriteResult(StrEnum):
    APPLIED = "applied"
    DUPLICATE = "duplicate"


class AggregateStore:
    def __init__(self, client: Any, aggregates_table: str, dedupe_table: str) -> None:
        self.client = client
        self.aggregates_table = aggregates_table
        self.dedupe_table = dedupe_table

    def record(self, event: Event) -> WriteResult:
        now = int(time.time())
        value = str(Decimal(repr(event.value)))
        try:
            self.client.transact_write_items(
                TransactItems=[
                    {
                        "Put": {
                            "TableName": self.dedupe_table,
                            "Item": {
                                "event_id": {"S": event.event_id},
                                "expires_at": {"N": str(now + DEDUPE_TTL_SECONDS)},
                            },
                            "ConditionExpression": "attribute_not_exists(event_id)",
                        }
                    },
                    {
                        "Update": {
                            "TableName": self.aggregates_table,
                            "Key": {"series": {"S": event.series}, "minute": {"S": event.minute}},
                            "UpdateExpression": _AGG_UPDATE,
                            "ExpressionAttributeValues": {
                                ":one": {"N": "1"},
                                ":v": {"N": value},
                                ":now": {"N": str(now)},
                            },
                        }
                    },
                ]
            )
        except ClientError as e:
            if _is_duplicate(e):
                return WriteResult.DUPLICATE
            raise
        return WriteResult.APPLIED

    def series(self, series: str, from_minute: str, to_minute: str) -> list[dict[str, Any]]:
        resp = self.client.query(
            TableName=self.aggregates_table,
            KeyConditionExpression="series = :s AND minute BETWEEN :a AND :b",
            ExpressionAttributeValues={
                ":s": {"S": series},
                ":a": {"S": from_minute},
                ":b": {"S": to_minute},
            },
        )
        out = []
        for item in resp["Items"]:
            count = int(item["event_count"]["N"])
            total = float(item["value_sum"]["N"])
            out.append(
                {
                    "minute": item["minute"]["S"],
                    "count": count,
                    "sum": total,
                    "avg": round(total / count, 4) if count else None,
                }
            )
        return out


def _is_duplicate(e: ClientError) -> bool:
    if e.response.get("Error", {}).get("Code") != "TransactionCanceledException":
        return False
    reasons = e.response.get("CancellationReasons") or []
    if reasons:
        return reasons[0].get("Code") == "ConditionalCheckFailed"
    return "ConditionalCheckFailed" in str(e)
