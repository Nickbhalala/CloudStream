"""Event schema and validation. Anything that fails here is bad data, not a transient error."""

from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

_ID = re.compile(r"^[A-Za-z0-9_.:-]{1,64}$")
_TYPE = re.compile(r"^[a-z][a-z0-9_]{0,31}$")
MAX_FUTURE = timedelta(minutes=5)
MAX_AGE = timedelta(days=7)


class InvalidEvent(ValueError):
    pass


@dataclass(frozen=True)
class Event:
    event_id: str
    source_id: str
    event_type: str
    value: float
    ts: datetime

    @property
    def minute(self) -> str:
        """Aggregation bucket, e.g. 2026-06-30T12:34."""
        return self.ts.strftime("%Y-%m-%dT%H:%M")

    @property
    def series(self) -> str:
        return f"{self.source_id}#{self.event_type}"

    def to_json(self) -> str:
        return json.dumps(
            {
                "event_id": self.event_id,
                "source_id": self.source_id,
                "event_type": self.event_type,
                "value": self.value,
                "ts": self.ts.isoformat().replace("+00:00", "Z"),
            }
        )


def parse_event(raw: bytes | str, now: datetime | None = None) -> Event:
    now = now or datetime.now(UTC)
    try:
        data: Any = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise InvalidEvent(f"not JSON: {e}") from e
    if not isinstance(data, dict):
        raise InvalidEvent("event must be a JSON object")

    for field in ("event_id", "source_id"):
        v = data.get(field)
        if not isinstance(v, str) or not _ID.match(v):
            raise InvalidEvent(f"{field} missing or invalid")
    etype = data.get("event_type")
    if not isinstance(etype, str) or not _TYPE.match(etype):
        raise InvalidEvent("event_type missing or invalid")

    value = data.get("value")
    if isinstance(value, bool) or not isinstance(value, int | float) or not math.isfinite(value):
        raise InvalidEvent("value must be a finite number")

    ts = _parse_ts(data.get("ts"))
    if ts > now + MAX_FUTURE:
        raise InvalidEvent("timestamp is in the future")
    if ts < now - MAX_AGE:
        raise InvalidEvent("timestamp is older than 7 days")
    return Event(data["event_id"], data["source_id"], etype, float(value), ts)


def _parse_ts(v: Any) -> datetime:
    if isinstance(v, int | float) and not isinstance(v, bool):
        return datetime.fromtimestamp(v / 1000, tz=UTC)  # epoch milliseconds
    if isinstance(v, str):
        try:
            ts = datetime.fromisoformat(v.replace("Z", "+00:00"))
        except ValueError as e:
            raise InvalidEvent(f"bad timestamp: {v!r}") from e
        return ts if ts.tzinfo else ts.replace(tzinfo=UTC)
    raise InvalidEvent("ts missing")
