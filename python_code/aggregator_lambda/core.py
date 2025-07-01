"""
Core business logic for the Data Aggregation Pipeline.

These functions are designed to be "pure" and testable, containing no
direct AWS SDK calls (unless passed in as arguments) and no global state.
They receive all dependencies, including the Powertools logger, from the main
handler in app.py, allowing them to be unit-tested in isolation.
"""

import json
import random
import time
from typing import Dict, List, cast
from urllib.parse import unquote_plus

from aws_lambda_powertools import Logger
from botocore.exceptions import ClientError

# Import boto3 stubs for full type-safety in function signatures
from mypy_boto3_dynamodb.service_resource import Table
from mypy_boto3_sqs.client import SQSClient

# Import the specific TypeDef required by the delete_message_batch API call.
from mypy_boto3_sqs.type_defs import DeleteMessageBatchRequestEntryTypeDef

from .model import FilterResult, SQSEventRecord, SQSMessageState


def filter_unique_objects(
    ddb_table: Table, ttl: int, sqs_messages: List[SQSEventRecord], logger: Logger
) -> FilterResult:
    """
    Deduplicates incoming S3 object keys against a DynamoDB table.

    This function implements a three-pass strategy to correctly handle SQS messages
    that may contain a mix of new and duplicate S3 object records.

    Args:
        ddb_table: The boto3 DynamoDB Table resource object.
        ttl: The Unix timestamp for when the idempotency records should expire.
        sqs_messages: The list of SQS message records from the Lambda event.
        logger: The Powertools Logger instance for structured logging.

    Returns:
        A FilterResult dataclass containing the lists of unique keys, messages to
        process, messages to delete, and a count of duplicates found.
    """
    message_states: Dict[str, SQSMessageState] = {}
    keys_to_check: Dict[str, List[str]] = {}

    for msg in sqs_messages:
        msg_id = msg["messageId"]
        try:
            body = json.loads(msg["body"])
            records = body.get("Records", [])
            if not records:
                logger.warning(
                    "SQS message with no records found.", extra={"messageId": msg_id}
                )
                continue

            message_states[msg_id] = SQSMessageState(
                msg=msg, total_records=len(records)
            )
            for record in records:
                key = unquote_plus(record["s3"]["object"]["key"])
                bucket = record["s3"]["bucket"]["name"]
                object_id = f"{bucket}/{key}"
                if object_id not in keys_to_check:
                    keys_to_check[object_id] = []
                keys_to_check[object_id].append(msg_id)
        except (json.JSONDecodeError, KeyError) as e:
            logger.error(
                "Malformed SQS message.", extra={"messageId": msg_id, "error": str(e)}
            )
            message_states[msg_id] = SQSMessageState(msg=msg, total_records=0)

    new_keys_found = set()
    for object_id in keys_to_check:
        try:
            ddb_table.put_item(
                Item={"ObjectID": object_id, "ExpiresAt": ttl},
                ConditionExpression="attribute_not_exists(ObjectID)",
            )
            new_keys_found.add(object_id)
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                logger.info(f"Duplicate object key detected: {object_id}")
            else:
                logger.exception("Unexpected DynamoDB error during idempotency check.")
                raise

    result = FilterResult()
    for object_id, msg_ids in keys_to_check.items():
        is_new = object_id in new_keys_found
        if is_new:
            result.unique_keys.append(object_id.split("/", 1)[1])
        else:
            result.duplicates_found += 1

        for msg_id in msg_ids:
            if is_new:
                message_states[msg_id].has_new_key = True

    for state in message_states.values():
        if state.has_new_key:
            result.messages_to_process.append(state.msg)
        else:
            result.messages_to_delete_as_duplicates.append(state.msg)

    return result


def delete_sqs_messages(
    sqs_client: SQSClient,
    queue_url: str,
    messages: List[SQSEventRecord],
    logger: Logger,
) -> int:
    """
    Deletes a list of messages from SQS, retrying failures with exponential backoff.

    This function deletes messages in batches of 10. If a batch deletion fails
    partially or completely, it will retry up to 3 times with increasing delays
    to handle transient network or API errors.

    Args:
        sqs_client: The boto3 SQS client resource.
        queue_url: The URL of the SQS queue.
        messages: A list of SQS message objects to delete.
        logger: The Powertools Logger instance for structured logging.

    Returns:
        The total count of messages that ultimately failed to be deleted after all retries.
    """
    if not messages:
        return 0

    total_failed_count = 0
    # Create a dictionary for quick lookup of receipt handles by message ID
    message_map = {m["messageId"]: m["receiptHandle"] for m in messages}

    # Process messages in batches of up to 10, as per the SQS API limit
    message_ids_to_delete = list(message_map.keys())
    for i in range(0, len(message_ids_to_delete), 10):
        batch_ids = message_ids_to_delete[i : i + 10]

        # Build the batch entries for the API call
        # This list will shrink on each successful partial deletion
        entries_to_delete = cast(
            List[DeleteMessageBatchRequestEntryTypeDef],
            [
                {"Id": msg_id, "ReceiptHandle": message_map[msg_id]}
                for msg_id in batch_ids
            ],
        )

        # Retry loop for the current batch
        for attempt in range(3):  # Attempt up to 3 times
            try:
                response = sqs_client.delete_message_batch(
                    QueueUrl=queue_url, Entries=entries_to_delete
                )

                if failed_batch := response.get("Failed"):
                    logger.warning(
                        "Partial failure in SQS delete batch.",
                        extra={
                            "attempt": attempt + 1,
                            "failed_messages": failed_batch,
                        },
                    )
                    # Rebuild the list of entries to retry with only the failed ones
                    failed_ids = {f["Id"] for f in failed_batch}
                    entries_to_delete = [
                        e for e in entries_to_delete if e["Id"] in failed_ids
                    ]
                else:
                    # Success: the entire batch was deleted
                    logger.info(
                        f"Successfully deleted {len(batch_ids)} SQS messages in batch."
                    )
                    entries_to_delete.clear()  # Empty the list to signify success
                    break  # Exit the retry loop for this batch

            except ClientError as e:
                logger.error(
                    "ClientError on SQS delete_message_batch.",
                    extra={"error": str(e), "attempt": attempt + 1},
                )
                # Let the loop continue to the next attempt

            # If we are here, it means the attempt failed. Wait before retrying.
            # Exponential backoff with jitter: 0.2s, 0.4s, 0.8s + random jitter
            wait_time = (0.2 * (2**attempt)) + random.uniform(0.0, 0.1)
            logger.info(f"Waiting {wait_time:.2f}s before SQS delete retry.")
            time.sleep(wait_time)

        # After all retries, any remaining entries in the list are permanent failures
        if entries_to_delete:
            final_failed_count = len(entries_to_delete)
            logger.critical(
                f"{final_failed_count} messages failed to be deleted after all retries.",
                extra={"failed_ids": [e["Id"] for e in entries_to_delete]},
            )
            total_failed_count += final_failed_count

    return total_failed_count
