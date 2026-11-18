import json
from datetime import UTC, datetime, timedelta

import pytest

from cloudstream.events import InvalidEvent, parse_event

NOW = datetime(2026, 6, 30, 12, 34, 56, tzinfo=UTC)


def raw(**overrides):
    base = {
        "event_id": "e-1",
        "source_id": "sensor-001",
        "event_type": "temperature",
        "value": 21.5,
        "ts": "2026-06-30T12:34:10Z",
    }
    base.update(overrides)
    return json.dumps({k: v for k, v in base.items() if v is not ...})


def test_valid_event():
    e = parse_event(raw(), now=NOW)
    assert e.minute == "2026-06-30T12:34"
    assert e.series == "sensor-001#temperature"
    assert parse_event(e.to_json(), now=NOW) == e


def test_epoch_millis_and_naive_iso():
    ms = int(datetime(2026, 6, 30, 12, 0, tzinfo=UTC).timestamp() * 1000)
    assert parse_event(raw(ts=ms), now=NOW).minute == "2026-06-30T12:00"
    assert parse_event(raw(ts="2026-06-30T12:01:00"), now=NOW).ts.tzinfo is UTC


@pytest.mark.parametrize(
    "payload,reason",
    [
        (b"\xff\xfe", "not JSON"),
        (b"[1, 2]", "JSON object"),
        (raw(event_id=...), "event_id"),
        (raw(source_id="has spaces"), "source_id"),
        (raw(event_type="Temperature!"), "event_type"),
        (raw(value=True), "finite number"),
        (raw(value="12"), "finite number"),
        (raw(value=float("nan")), "finite number"),
        (raw(ts="yesterday"), "bad timestamp"),
        (raw(ts=...), "ts missing"),
        (raw(ts=(NOW + timedelta(minutes=10)).isoformat()), "future"),
        (raw(ts=(NOW - timedelta(days=8)).isoformat()), "older than 7 days"),
    ],
)
def test_invalid_events(payload, reason):
    with pytest.raises(InvalidEvent, match=reason):
        parse_event(payload, now=NOW)
