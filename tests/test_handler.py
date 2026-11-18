import base64
import json
from datetime import UTC, datetime

from cloudstream.events import Event
from cloudstream.handler import lambda_handler, process_batch
from cloudstream.metrics import Metrics

NOW = datetime.now(UTC)


def record(seq: int, payload: bytes | str) -> dict:
    if isinstance(payload, str):
        payload = payload.encode()
    return {
        "kinesis": {
            "sequenceNumber": f"{seq:020d}",
            "partitionKey": "pk",
            "data": base64.b64encode(payload).decode(),
            "approximateArrivalTimestamp": NOW.timestamp() - 2,
        }
    }


def event(eid: str, value: float = 1.0) -> str:
    return Event(eid, "sensor-1", "temp", value, NOW).to_json()


def run(aws, batch, store=None):
    lines: list[str] = []
    m = Metrics({"Function": "test"}, sink=lines.append)
    out = process_batch(batch, store or aws["store"], aws["sqs"], aws["dlq_url"], m, now=NOW)
    m.flush()
    return out, json.loads(lines[0])


def stored_count(aws) -> int:
    rows = aws["store"].series("sensor-1#temp", "0000", "9999")
    return sum(r["count"] for r in rows)


def test_valid_duplicate_and_invalid_records(aws):
    batch = {
        "Records": [
            record(1, event("a")),
            record(2, event("b")),
            record(3, event("a")),
            record(4, "{not json"),
            record(5, event("c")),
        ]
    }
    out, metrics = run(aws, batch)

    assert out == {"batchItemFailures": []}
    assert stored_count(aws) == 3
    assert metrics["RecordsReceived"] == 5
    assert metrics["AppliedRecords"] == 3
    assert metrics["DuplicateRecords"] == 1
    assert metrics["InvalidRecords"] == 1
    assert 1500 < metrics["MaxRecordAgeMs"] < 60_000

    msgs = aws["sqs"].receive_message(QueueUrl=aws["dlq_url"])["Messages"]
    body = json.loads(msgs[0]["Body"])
    assert body["sequenceNumber"] == f"{4:020d}" and "not JSON" in body["reason"]


def test_transient_failure_reports_record_and_retry_is_exactly_once(aws):
    real = aws["store"]

    class FlakyStore:
        """Fails on the 3rd write, like a throttled DynamoDB partition."""

        def __init__(self):
            self.calls = 0

        def record(self, e):
            self.calls += 1
            if self.calls == 3:
                raise RuntimeError("ProvisionedThroughputExceeded")
            return real.record(e)

    batch = {"Records": [record(i, event(f"e{i}", 10)) for i in range(1, 6)]}
    out, metrics = run(aws, batch, store=FlakyStore())
    assert out == {"batchItemFailures": [{"itemIdentifier": f"{3:020d}"}]}
    assert stored_count(aws) == 2  # e1, e2 committed; stopped at e3
    assert metrics["RetriedRecords"] == 3

    # Lambda retries from record 3. Even if it replays the whole batch, nothing doubles.
    out, metrics = run(aws, batch)
    assert out == {"batchItemFailures": []}
    assert stored_count(aws) == 5
    assert metrics["DuplicateRecords"] == 2 and metrics["AppliedRecords"] == 3


def test_lambda_entrypoint_emits_emf(aws, clean_env, monkeypatch, capsys):
    monkeypatch.setenv("AGGREGATES_TABLE", aws["agg"])
    monkeypatch.setenv("DEDUPE_TABLE", aws["dedupe"])
    monkeypatch.setenv("DLQ_URL", aws["dlq_url"])

    class Ctx:
        function_name = "cloudstream-processor"

    out = lambda_handler({"Records": [record(1, event("z"))]}, Ctx())
    assert out == {"batchItemFailures": []}
    emf = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert emf["_aws"]["CloudWatchMetrics"][0]["Namespace"] == "CloudStream"
    assert emf["_aws"]["CloudWatchMetrics"][0]["Dimensions"] == [["Function"]]
    assert emf["Function"] == "cloudstream-processor" and emf["AppliedRecords"] == 1
