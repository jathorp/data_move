import hashlib
import json
import logging
import os
import queue
import tarfile
import tempfile
import threading
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import boto3
import botocore.config
import botocore.exceptions
from botocore.client import BaseClient


# --- 1. SETUP: Configuration, Validation, and Clients ---

def get_env_var(name: str, default: Optional[str] = None) -> str:
    """Gets an environment variable or raises an error if not found and no default is given."""
    value = os.environ.get(name, default)
    if value is None:
        raise ValueError(f"FATAL: Environment variable '{name}' is not set.")
    return value


# Validate required env vars on import to fail fast
LANDING_BUCKET = get_env_var("LANDING_BUCKET")
MINIO_SECRET_ID = get_env_var("MINIO_SECRET_ID")
MINIO_BUCKET = get_env_var("MINIO_BUCKET")
QUEUE_URL = get_env_var("QUEUE_URL")
IDEMPOTENCY_TABLE = get_env_var("IDEMPOTENCY_TABLE")

# Configuration with defaults
ENVIRONMENT = get_env_var("ENVIRONMENT", "dev")
LOG_LEVEL = get_env_var("LOG_LEVEL", "INFO").upper()
IDEMPOTENCY_TTL_HOURS = int(get_env_var("IDEMPOTENCY_TTL_HOURS", "24"))
SECRET_CACHE_TTL_SECONDS = int(get_env_var("SECRET_CACHE_TTL_SECONDS", "300"))
MINIO_SSE_TYPE = get_env_var("MINIO_SSE_TYPE", "AES256")
MAX_FETCH_WORKERS = int(get_env_var("MAX_FETCH_WORKERS", "16"))
# (P-1) Increase queue depth to better buffer between network and CPU tasks.
QUEUE_DEPTH_FACTOR = int(get_env_var("QUEUE_DEPTH_FACTOR", "4"))
SPOOL_MAX_MEMORY_BYTES = int(get_env_var("SPOOL_MAX_MEMORY_BYTES", "104857600"))  # 100 MiB

# Setup structured logging
logger = logging.getLogger()
logger.setLevel(LOG_LEVEL)

BOTO_CONFIG_RETRYABLE = botocore.config.Config(retries={"max_attempts": 5, "mode": "adaptive"})

S3 = boto3.client("s3")
SQS = boto3.client("sqs")
DDB = boto3.resource("dynamodb")
SECRETS = boto3.client("secretsmanager")

MINIO: Optional[BaseClient] = None
MINIO_SECRET_CACHE = {"data": None, "timestamp": datetime.min.replace(tzinfo=timezone.utc)}


# --- 2. DATA CLASSES & HELPERS ---

@dataclass
class SQSMessageState:
    msg: Dict[str, Any]
    total_records: int
    has_new_key: bool = False


@dataclass
class FilterResult:
    unique_keys: List[str] = field(default_factory=list)
    messages_to_process: List[Dict] = field(default_factory=list)
    messages_to_delete_as_duplicates: List[Dict] = field(default_factory=list)
    duplicates_found: int = 0


class HashingStreamWrapper:
    """Computes a hash on a stream as it's read."""

    def __init__(self, stream):
        self._stream = stream
        self._hasher = hashlib.sha256()

    def read(self, size=-1):
        chunk = self._stream.read(size)
        if chunk: self._hasher.update(chunk)
        return chunk

    def hexdigest(self):
        return self._hasher.hexdigest()


# --- 3. CORE LOGIC FUNCTIONS ---

def get_minio_client(force_refresh: bool = False) -> BaseClient:
    global MINIO, MINIO_SECRET_CACHE
    now = datetime.now(timezone.utc)
    cache_expiry = MINIO_SECRET_CACHE["timestamp"] + timedelta(seconds=SECRET_CACHE_TTL_SECONDS)
    if MINIO and not force_refresh and now < cache_expiry:
        return MINIO

    logger.info(f"Refreshing MinIO client credentials. Force refresh: {force_refresh}")
    secret_value = SECRETS.get_secret_value(SecretId=MINIO_SECRET_ID)
    secret_data = json.loads(secret_value["SecretString"])
    MINIO_SECRET_CACHE = {"data": secret_data, "timestamp": now}

    MINIO = boto3.client("s3", endpoint_url=secret_data["endpoint_url"], aws_access_key_id=secret_data["access_key"],
                         aws_secret_access_key=secret_data["secret_key"], config=BOTO_CONFIG_RETRYABLE)
    return MINIO


def filter_unique_objects(sqs_messages: List[Dict]) -> FilterResult:
    """
    (Fixes C-1) Deduplicates S3 keys against DynamoDB using conditional PutItem calls.
    """
    table = DDB.Table(IDEMPOTENCY_TABLE)
    ttl = int((datetime.now(timezone.utc) + timedelta(hours=IDEMPOTENCY_TTL_HOURS)).timestamp())

    message_states: Dict[str, SQSMessageState] = {}
    keys_to_check: Dict[str, List[str]] = {}

    for msg in sqs_messages:
        msg_id = msg["messageId"]
        try:
            body = json.loads(msg["body"])
            records = body.get("Records", [])
            if not records: continue

            message_states[msg_id] = SQSMessageState(msg=msg, total_records=len(records))
            for record in records:
                key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])
                object_id = f"{record['s3']['bucket']['name']}/{key}"
                if object_id not in keys_to_check: keys_to_check[object_id] = []
                keys_to_check[object_id].append(msg_id)
        except (json.JSONDecodeError, KeyError) as e:
            logger.error(f"Malformed SQS message {msg_id}, will be deleted. Error: {e}")
            message_states[msg_id] = SQSMessageState(msg=msg, total_records=0)

    new_keys_found = set()
    # (C-1) Reverted from batch_writer to single put_item to support ConditionExpression.
    for object_id in keys_to_check:
        try:
            table.put_item(
                Item={"ObjectID": object_id, "ExpiresAt": ttl},
                ConditionExpression="attribute_not_exists(ObjectID)"
            )
            new_keys_found.add(object_id)
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
                raise

    result = FilterResult()
    for object_id, msg_ids in keys_to_check.items():
        is_new = object_id in new_keys_found
        if is_new:
            result.unique_keys.append(object_id.split('/', 1)[1])
        else:
            result.duplicates_found += 1

        for msg_id in msg_ids:
            if is_new: message_states[msg_id].has_new_key = True

    for state in message_states.values():
        if state.has_new_key:
            result.messages_to_process.append(state.msg)
        else:
            result.messages_to_delete_as_duplicates.append(state.msg)

    return result


def stream_archive_to_minio(s3_keys: List[str], dest_key: str) -> str:
    """(Fixes C-2) Thread-safe, robust streaming archiver using a sentinel pattern."""
    minio_client = get_minio_client()
    data_queue: queue.Queue = queue.Queue(maxsize=MAX_FETCH_WORKERS * QUEUE_DEPTH_FACTOR)
    error_queue: queue.Queue = queue.Queue()

    def _fetcher(key: str):
        try:
            s3_obj = S3.get_object(Bucket=LANDING_BUCKET, Key=key)
            data_queue.put((key, s3_obj["Body"], s3_obj["ContentLength"]), timeout=10)
        except Exception as e:
            # (P-2) Log every failure immediately for full visibility.
            logger.exception(f"Fetcher thread failed for key {key}")
            error_queue.put(e)

    def _writer(spooled_file: tempfile.SpooledTemporaryFile):
        try:
            with tarfile.open(fileobj=spooled_file, mode="w:gz") as tar:
                # (C-2) Loop until a sentinel value (None) is received.
                while True:
                    item = data_queue.get()
                    if item is None:
                        break
                    key, body, size = item
                    with body:
                        tarinfo = tarfile.TarInfo(name=key)
                        tarinfo.size = size
                        tar.addfile(tarinfo, body)
                    data_queue.task_done()
        except Exception as e:
            logger.exception("Writer thread failed")
            error_queue.put(e)

    with tempfile.SpooledTemporaryFile(max_size=SPOOL_MAX_MEMORY_BYTES, mode='w+b') as spooled_archive:
        writer_thread = threading.Thread(target=_writer, args=(spooled_archive,))
        writer_thread.start()

        with ThreadPoolExecutor(max_workers=MAX_FETCH_WORKERS) as executor:
            for s3_key in s3_keys:
                executor.submit(_fetcher, s3_key)

        # (C-2) Signal to the writer that all fetch tasks have been submitted.
        data_queue.put(None)
        writer_thread.join(timeout=300)

        if not error_queue.empty(): raise error_queue.get()
        if writer_thread.is_alive(): raise TimeoutError("Archive writer thread timed out.")

        # (P-3) Add observability for large archives that require disk spooling.
        if spooled_archive._rolled_to_disk:  # type: ignore
            logger.warning(f"Archive size exceeded {SPOOL_MAX_MEMORY_BYTES} bytes; spooled to disk.")

        spooled_archive.seek(0)
        hashing_stream = HashingStreamWrapper(spooled_archive)
        extra_args = {"ServerSideEncryption": MINIO_SSE_TYPE} if MINIO_SSE_TYPE != "NONE" else {}

        minio_client.upload_fileobj(hashing_stream, MINIO_BUCKET, dest_key, ExtraArgs=extra_args)
        return hashing_stream.hexdigest()


def delete_sqs_messages(messages: List[Dict]) -> int:
    """Deletes a list of messages from SQS, handling batching and retries."""
    if not messages: return 0
    failed_count = 0
    to_delete = [{"Id": m["messageId"], "ReceiptHandle": m["receiptHandle"]} for m in messages]

    for i in range(0, len(to_delete), 10):
        batch = to_delete[i:i + 10]
        try:
            response = SQS.delete_message_batch(QueueUrl=QUEUE_URL, Entries=batch)
            if failed := response.get("Failed"):
                failed_count += len(failed)
                logger.error(f"Failed to delete SQS messages: {failed}")
        except botocore.exceptions.ClientError as e:
            failed_count += len(batch)
            logger.error(f"Could not delete message batch due to client error: {e}")

    return failed_count


def emit_metrics(status: str, payload: Dict):
    """
    Formats and logs metrics in CloudWatch Embedded Metric Format (EMF).
    Note (P-4): Dashboards and alarms should filter/group by the 'Environment' dimension.
    """
    base_metrics = {"FilesProcessed": payload.get("source_files", 0),
                    "DuplicateFilesSkipped": payload.get("duplicates_skipped", 0),
                    "DeleteFailures": payload.get("delete_failures", 0)}
    if "latency_ms" in payload: base_metrics["ProcessingLatencyMs"] = payload["latency_ms"]

    emf_payload = {"_aws": {"Timestamp": int(datetime.now(timezone.utc).timestamp() * 1000), "CloudWatchMetrics": [
        {"Namespace": "DataMovePipeline", "Dimensions": [["Environment"]],
         "Metrics": [{"Name": k, "Unit": "Milliseconds" if "Latency" in k else "Count"} for k in
                     base_metrics.keys()]}]}, "Environment": ENVIRONMENT, "Status": status, **base_metrics, **payload}
    logger.info(json.dumps(emf_payload))


# --- 4. LAMBDA HANDLER ---

def _build_response(status_code: int, body: Dict[str, Any]) -> Dict[str, Any]:
    """(S-1) Centralized response builder for consistent exit path."""
    return {"statusCode": status_code, "body": json.dumps(body)}


def handler(event: Dict, context: Any):
    start_time = datetime.now(timezone.utc)
    sqs_messages = event.get("Records", [])
    if not sqs_messages:
        return _build_response(200, {"message": "No messages to process."})

    logger.info(f"Received {len(sqs_messages)} messages to process.")

    try:
        filter_result = filter_unique_objects(sqs_messages)
        delete_sqs_messages(filter_result.messages_to_delete_as_duplicates)

        if not filter_result.unique_keys:
            logger.info("All messages in this batch were duplicates. Exiting.")
            return _build_response(200, {"message": "All messages were duplicates."})

        dest_key = f"archive/{start_time.strftime('%Y/%m/%d/%H%M%S')}-{context.aws_request_id}.tar.gz"
        digest = stream_archive_to_minio(filter_result.unique_keys, dest_key)

        total_delete_failures = delete_sqs_messages(filter_result.messages_to_process)
        if total_delete_failures > 0:
            logger.error(f"{total_delete_failures} messages could not be deleted after successful processing.")

        latency_ms = int((datetime.now(timezone.utc) - start_time).total_seconds() * 1000)
        log_payload = {"output_key": dest_key, "source_files": len(filter_result.unique_keys),
                       "duplicates_skipped": filter_result.duplicates_found, "delete_failures": total_delete_failures,
                       "sha256_checksum": digest, "latency_ms": latency_ms}
        emit_metrics("Success", log_payload)
        logger.info(f"Successfully processed batch: {log_payload['output_key']}")
        return _build_response(200, log_payload)

    except Exception as e:
        latency_ms = int((datetime.now(timezone.utc) - start_time).total_seconds() * 1000)
        error_payload = {"error_type": type(e).__name__, "error_message": str(e), "latency_ms": latency_ms}
        emit_metrics("Failure", error_payload)
        logger.error(f"Processing failed: {json.dumps(error_payload)}", exc_info=True)
        raise