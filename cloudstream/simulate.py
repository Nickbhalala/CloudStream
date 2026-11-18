"""Run the whole pipeline locally with moto standing in for AWS. No account needed.

producer -> Kinesis -> handler (as Lambda would call it) -> DynamoDB / SQS DLQ
"""

from __future__ import annotations

import argparse
import json
import os
from collections import Counter

STREAM, AGG, DEDUPE, DLQ = (
    "cloudstream-events",
    "cloudstream-aggregates",
    "cloudstream-dedupe",
    "cloudstream-dlq",
)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="cloudstream-simulate")
    p.add_argument("--events", type=int, default=2000)
    p.add_argument("--duplicates", type=int, default=50, help="events re-sent to the stream")
    p.add_argument("--bad", type=int, default=10, help="malformed records sent")
    p.add_argument("--sources", type=int, default=5)
    args = p.parse_args(argv)

    try:
        from moto import mock_aws
    except ImportError:
        print("simulate needs the dev extras: pip install -e '.[dev]'")
        return 2
    import boto3

    from .aws_local import create_resources, read_as_lambda_events
    from .handler import process_batch
    from .metrics import Metrics
    from .producer import put_events, synthetic_events
    from .store import AggregateStore

    for k, v in {
        "AWS_ACCESS_KEY_ID": "testing",
        "AWS_SECRET_ACCESS_KEY": "testing",
        "AWS_DEFAULT_REGION": "us-east-1",
    }.items():
        os.environ.setdefault(k, v)

    with mock_aws():
        session = boto3.Session(region_name="us-east-1")
        dlq_url = create_resources(session, STREAM, AGG, DEDUPE, DLQ)["dlq_url"]
        kinesis = session.client("kinesis")

        events = list(synthetic_events(args.events, args.sources, seed=1))
        put_events(kinesis, STREAM, events + events[: args.duplicates])
        for i in range(args.bad):
            kinesis.put_record(
                StreamName=STREAM, Data=b'{"event_id": "broken"', PartitionKey=f"bad-{i}"
            )

        store = AggregateStore(session.client("dynamodb"), AGG, DEDUPE)
        totals: Counter[str] = Counter()

        def collect(line: str) -> None:
            data = json.loads(line)
            for m in data["_aws"]["CloudWatchMetrics"][0]["Metrics"]:
                if m["Unit"] == "Count":
                    totals[m["Name"]] += data[m["Name"]]

        for batch in read_as_lambda_events(kinesis, STREAM):
            metrics = Metrics({"Function": "simulate"}, sink=collect)
            process_batch(batch, store, session.client("sqs"), dlq_url, metrics)
            metrics.flush()

        print("metrics:", dict(sorted(totals.items())))
        dlq = session.client("sqs").get_queue_attributes(
            QueueUrl=dlq_url, AttributeNames=["ApproximateNumberOfMessages"]
        )
        print("dead-lettered:", dlq["Attributes"]["ApproximateNumberOfMessages"])

        by_series = Counter(e.series for e in events)
        series, expected = by_series.most_common(1)[0]
        rows = store.series(series, "0000", "9999")
        stored = sum(r["count"] for r in rows)
        print(
            f"{series}: {stored} events in DynamoDB across {len(rows)} minute(s) "
            f"(sent {expected} unique)"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
