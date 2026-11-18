from cloudstream.aws_local import read_as_lambda_events
from cloudstream.producer import put_events, synthetic_events


class PartiallyFailingKinesis:
    """Throttles every other record on the first attempt of each batch."""

    def __init__(self, always_fail: bool = False):
        self.calls: list[int] = []
        self.always_fail = always_fail

    def put_records(self, StreamName, Records):  # noqa: N803 (boto3 casing)
        self.calls.append(len(Records))
        first_try = len(self.calls) % 2 == 1
        results = []
        for i, _ in enumerate(Records):
            fail = self.always_fail or (first_try and i % 2 == 0)
            results.append(
                {"ErrorCode": "ProvisionedThroughputExceededException"}
                if fail
                else {"SequenceNumber": "1", "ShardId": "s"}
            )
        return {"FailedRecordCount": sum("ErrorCode" in r for r in results), "Records": results}


def test_retries_only_failed_entries():
    client = PartiallyFailingKinesis()
    result = put_events(client, "s", synthetic_events(10, seed=1), sleep=lambda _: None)
    assert result == {"sent": 10, "failed": 0}
    assert client.calls == [10, 5]


def test_batches_at_500_and_reports_permanent_failures():
    client = PartiallyFailingKinesis(always_fail=True)
    result = put_events(
        client, "s", synthetic_events(600, seed=1), max_retries=2, sleep=lambda _: None
    )
    assert result == {"sent": 0, "failed": 600}
    assert client.calls == [500, 500, 500, 100, 100, 100]


def test_round_trip_through_kinesis(aws):
    events = list(synthetic_events(25, sources=3, seed=2))
    assert put_events(aws["kinesis"], aws["stream"], events) == {"sent": 25, "failed": 0}
    batches = read_as_lambda_events(aws["kinesis"], aws["stream"])
    records = [r for b in batches for r in b["Records"]]
    assert len(records) == 25
    assert {r["kinesis"]["partitionKey"] for r in records} == {e.source_id for e in events}
