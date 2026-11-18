"""Creates the stack's resources in an AWS-API-compatible backend (moto in tests/simulation).

Mirrors template.yaml: same keys, TTL attribute and on-demand billing.
"""

from __future__ import annotations

import base64
from datetime import datetime
from typing import Any


def create_resources(
    session: Any, stream: str, agg_table: str, dedupe_table: str, dlq: str, shards: int = 1
) -> dict[str, str]:
    kinesis = session.client("kinesis")
    ddb = session.client("dynamodb")
    sqs = session.client("sqs")
    kinesis.create_stream(StreamName=stream, ShardCount=shards)
    ddb.create_table(
        TableName=agg_table,
        BillingMode="PAY_PER_REQUEST",
        AttributeDefinitions=[
            {"AttributeName": "series", "AttributeType": "S"},
            {"AttributeName": "minute", "AttributeType": "S"},
        ],
        KeySchema=[
            {"AttributeName": "series", "KeyType": "HASH"},
            {"AttributeName": "minute", "KeyType": "RANGE"},
        ],
    )
    ddb.create_table(
        TableName=dedupe_table,
        BillingMode="PAY_PER_REQUEST",
        AttributeDefinitions=[{"AttributeName": "event_id", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "event_id", "KeyType": "HASH"}],
    )
    ddb.update_time_to_live(
        TableName=dedupe_table,
        TimeToLiveSpecification={"Enabled": True, "AttributeName": "expires_at"},
    )
    return {"dlq_url": sqs.create_queue(QueueName=dlq)["QueueUrl"]}


def read_as_lambda_events(kinesis: Any, stream: str, batch_size: int = 100) -> list[dict]:
    """Read every record in the stream and shape it like Lambda's Kinesis event."""
    events = []
    shards = kinesis.describe_stream(StreamName=stream)["StreamDescription"]["Shards"]
    for shard in shards:
        it = kinesis.get_shard_iterator(
            StreamName=stream, ShardId=shard["ShardId"], ShardIteratorType="TRIM_HORIZON"
        )["ShardIterator"]
        while it:
            resp = kinesis.get_records(ShardIterator=it, Limit=batch_size)
            if not resp["Records"]:
                break
            events.append(
                {"Records": [_lambda_record(r, shard["ShardId"]) for r in resp["Records"]]}
            )
            it = resp.get("NextShardIterator")
    return events


def _lambda_record(r: dict, shard_id: str) -> dict:
    arrival: datetime = r["ApproximateArrivalTimestamp"]
    return {
        "eventSource": "aws:kinesis",
        "eventID": f"{shard_id}:{r['SequenceNumber']}",
        "kinesis": {
            "partitionKey": r["PartitionKey"],
            "sequenceNumber": r["SequenceNumber"],
            "data": base64.b64encode(r["Data"]).decode(),
            "approximateArrivalTimestamp": arrival.timestamp(),
        },
    }
