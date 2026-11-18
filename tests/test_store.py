from datetime import UTC, datetime

import pytest
from botocore.exceptions import ClientError

from cloudstream.events import Event
from cloudstream.store import AggregateStore, WriteResult


def ev(eid: str, value: float, minute: int = 0) -> Event:
    return Event(eid, "s1", "temp", value, datetime(2026, 6, 30, 12, minute, tzinfo=UTC))


def test_exactly_once_per_event_id(aws):
    store = aws["store"]
    assert store.record(ev("a", 10)) is WriteResult.APPLIED
    assert store.record(ev("b", 20)) is WriteResult.APPLIED
    assert store.record(ev("a", 10)) is WriteResult.DUPLICATE
    assert store.record(ev("c", 5, minute=1)) is WriteResult.APPLIED

    rows = store.series("s1#temp", "2026-06-30T12:00", "2026-06-30T12:59")
    assert rows == [
        {"minute": "2026-06-30T12:00", "count": 2, "sum": 30.0, "avg": 15.0},
        {"minute": "2026-06-30T12:01", "count": 1, "sum": 5.0, "avg": 5.0},
    ]


def test_dedupe_items_get_ttl(aws):
    aws["store"].record(ev("ttl-check", 1))
    item = aws["ddb"].get_item(TableName=aws["dedupe"], Key={"event_id": {"S": "ttl-check"}})
    expires = int(item["Item"]["expires_at"]["N"])
    assert expires > datetime.now(UTC).timestamp() + 6 * 24 * 3600


def test_other_errors_propagate():
    class Throttled:
        def transact_write_items(self, **_):
            raise ClientError(
                {
                    "Error": {
                        "Code": "ProvisionedThroughputExceededException",
                        "Message": "slow down",
                    }
                },
                "TransactWriteItems",
            )

    with pytest.raises(ClientError):
        AggregateStore(Throttled(), "a", "d").record(ev("x", 1))
