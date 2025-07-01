"""
A factory module for creating and providing boto3 clients.

This module is the core of the Dependency Injection (DI) pattern for the
application. It allows the main handler to receive either real AWS clients
or mocked clients during testing, based on the presence of an environment
variable. This makes the application's business logic fully testable
without making real AWS calls.
"""

import logging
import os
from typing import Tuple

import boto3
import botocore.config

from mypy_boto3_dynamodb.service_resource import DynamoDBServiceResource
from mypy_boto3_s3 import S3Client
from mypy_boto3_secretsmanager import SecretsManagerClient
from mypy_boto3_sqs import SQSClient

logger = logging.getLogger(__name__)

# A shared, robust retry configuration for boto3 clients that need to be
# resilient to transient network or server-side errors. This is used by
# the MinIO client created in app.py.
BOTO_CONFIG_RETRYABLE = botocore.config.Config(
    retries={"max_attempts": 5, "mode": "adaptive"}
)


def get_boto_clients() -> Tuple[
    S3Client, SQSClient, DynamoDBServiceResource, SecretsManagerClient
]:
    """
    Returns a tuple of essential AWS service clients.

    This factory provides the core mechanism for dependency injection. It inspects
    the environment for a `USE_MOTO` flag. If present, it's assumed that `moto`
    is active and will intercept the `boto3` calls to return mocked clients.
    Otherwise, it creates real AWS clients.

    The AWS region is explicitly read from the environment to ensure consistent
    and predictable behavior across all clients.

    Returns:
        A tuple containing initialized boto3 clients in the following order:
        (s3_client, sqs_client, dynamodb_resource, secretsmanager_client)
    """
    # Explicitly use the region defined in the Lambda execution environment.
    # This is safer than relying on boto3's default resolution behavior.
    aws_region = os.environ.get("AWS_REGION")
    if not aws_region:
        # This should ideally not happen in a real Lambda environment.
        logger.warning("AWS_REGION not set, boto3 will attempt to resolve it.")

    # In a test run with the moto fixture, this log confirms DI is active.
    if os.environ.get("USE_MOTO"):
        logger.info("MOTO ENABLED: Returning mocked AWS clients.")

    # In production, this creates real clients.
    # In a test run with the moto fixture, these calls are hijacked to create mocked clients.
    s3_client: S3Client = boto3.client("s3", region_name=aws_region)
    sqs_client: SQSClient = boto3.client("sqs", region_name=aws_region)
    dynamodb_resource: DynamoDBServiceResource = boto3.resource(
        "dynamodb", region_name=aws_region
    )
    secretsmanager_client: SecretsManagerClient = boto3.client(
        "secretsmanager", region_name=aws_region
    )

    return (
        s3_client,
        sqs_client,
        dynamodb_resource,
        secretsmanager_client,
    )
