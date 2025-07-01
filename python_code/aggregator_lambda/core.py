# aggregator_lambda/core.py

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List

import botocore.exceptions

# Corrected: Import shared data classes from the new model.py
from .model import FilterResult, SQSMessageState


def filter_unique_objects(
    ddb_table: Any, ttl: int, sqs_messages: List[Dict]
) -> FilterResult:
    """Deduplicates S3 keys against DynamoDB using conditional PutItem calls."""
    message_states: Dict[str, SQSMessageState] = {}
    keys_to_check: Dict[str, List[str]] = {}

    for msg in sqs_messages:
        msg_id = msg["messageId"]
        try:
            body = json.loads(msg["body"])
            records = body.get("Records", [])
            if not records:
                continue

            message_states[msg_id] = SQSMessageState(
                msg=msg, total_records=len(records)
            )
            for record in records:
                key = record["s3"]["object"]["key"]
                bucket = record["s3"]["bucket"]["name"]
                object_id = f"{bucket}/{key}"
                if object_id not in keys_to_check:
                    keys_to_check[object_id] = []
                keys_to_check[object_id].append(msg_id)
        except (json.JSONDecodeError, KeyError) as e:
            logging.error(
                f"Malformed SQS message {msg_id}, will be deleted. Error: {e}"
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
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
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


def delete_sqs_messages(sqs_client: Any, queue_url: str, messages: List[Dict]) -> int:
    """Deletes messages from SQS. Pure function."""
    if not messages:
        return 0
    failed_count = 0
    to_delete = [
        {"Id": m["messageId"], "ReceiptHandle": m["receiptHandle"]} for m in messages
    ]

    for i in range(0, len(to_delete), 10):
        batch = to_delete[i : i + 10]
        try:
            response = sqs_client.delete_message_batch(
                QueueUrl=queue_url, Entries=batch
            )
            if failed := response.get("Failed"):
                failed_count += len(failed)
                logging.error(f"Failed to delete SQS messages: {failed}")
        except botocore.exceptions.ClientError as e:
            failed_count += len(batch)
            logging.error(f"Could not delete message batch due to client error: {e}")

    return failed_count


def emit_metrics(environment: str, status: str, payload: Dict):
    """Formats and logs metrics in CloudWatch Embedded Metric Format (EMF)."""
    base_metrics = {
        "FilesProcessed": payload.get("source_files", 0),
        "DuplicateFilesSkipped": payload.get("duplicates_skipped", 0),
        "DeleteFailures": payload.get("delete_failures", 0),
    }
    if "latency_ms" in payload:
        base_metrics["ProcessingLatencyMs"] = payload["latency_ms"]

    emf_payload = {
        "_aws": {
            "Timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
            "CloudWatchMetrics": [
                {
                    "Namespace": "DataMovePipeline",
                    "Dimensions": [["Environment"]],
                    "Metrics": [
                        {
                            "Name": k,
                            "Unit": "Milliseconds" if "Latency" in k else "Count",
                        }
                        for k in base_metrics.keys()
                    ],
                }
            ],
        },
        "Environment": environment,
        "Status": status,
        **base_metrics,
        **payload,
    }
    logging.info(json.dumps(emf_payload))
