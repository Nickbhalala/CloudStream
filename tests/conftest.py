import os

import boto3
import pytest
from moto import mock_aws

from cloudstream.aws_local import create_resources
from cloudstream.store import AggregateStore

NAMES = {"stream": "test-events", "agg": "test-agg", "dedupe": "test-dedupe", "dlq": "test-dlq"}


@pytest.fixture(autouse=True)
def aws_env(monkeypatch):
    for k, v in {
        "AWS_ACCESS_KEY_ID": "testing",
        "AWS_SECRET_ACCESS_KEY": "testing",
        "AWS_SESSION_TOKEN": "testing",
        "AWS_DEFAULT_REGION": "us-east-1",
    }.items():
        monkeypatch.setenv(k, v)
    monkeypatch.delenv("AWS_PROFILE", raising=False)


@pytest.fixture
def aws():
    with mock_aws():
        session = boto3.Session(region_name="us-east-1")
        urls = create_resources(
            session, NAMES["stream"], NAMES["agg"], NAMES["dedupe"], NAMES["dlq"]
        )
        yield {
            "session": session,
            "kinesis": session.client("kinesis"),
            "ddb": session.client("dynamodb"),
            "sqs": session.client("sqs"),
            "dlq_url": urls["dlq_url"],
            "store": AggregateStore(session.client("dynamodb"), NAMES["agg"], NAMES["dedupe"]),
            **NAMES,
        }


@pytest.fixture
def clean_env():
    """Ensure handler module-level client cache doesn't leak between tests."""
    from cloudstream import handler

    handler._clients.cache_clear()
    yield
    handler._clients.cache_clear()
    for k in ("AGGREGATES_TABLE", "DEDUPE_TABLE", "DLQ_URL"):
        os.environ.pop(k, None)
