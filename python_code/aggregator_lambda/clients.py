import os

import boto3
import botocore.config

# This config is used for the MinIO client, which is created separately in the handler.
BOTO_CONFIG_RETRYABLE = botocore.config.Config(
    retries={"max_attempts": 5, "mode": "adaptive"}
)


def get_boto_clients():
    """
    Returns a tuple of (s3, sqs, ddb, secrets) clients.

    In a test environment (indicated by the USE_MOTO env var), this will
    return clients mocked by the Moto library. In production, it returns
    real AWS clients.
    """
    # In tests, moto is started via a pytest fixture (see conftest.py)
    # which intercepts these boto3 calls.
    if os.environ.get("USE_MOTO"):
        # When USE_MOTO is set, these calls are hijacked by moto to return mock clients.
        # This check is primarily for clarity; the interception happens regardless.
        print("MOTO ENABLED: Returning mocked AWS clients.")

    # In production, this creates real clients.
    # In a test run with the moto fixture, this creates mocked clients.
    return (
        boto3.client("s3"),
        boto3.client("sqs"),
        boto3.resource("dynamodb"),
        boto3.client("secretsmanager"),
    )
