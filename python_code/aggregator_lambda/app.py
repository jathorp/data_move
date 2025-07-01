"""
Main AWS Lambda handler for the Data Aggregation Pipeline.

This module serves as the primary entry point and orchestrator for the function.
Its responsibilities include:
  - Loading and validating configuration from environment variables.
  - Initializing and caching stateful clients (e.g., the MinIO client).
  - Receiving events from the SQS trigger.
  - Calling pure, testable business logic functions from the 'core' module.
  - Handling the complex, multi-threaded streaming archive process.
  - Managing the overall success/failure state and emitting the final metrics.
"""

import hashlib
import json
import logging
import os
import queue
import tarfile
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

import boto3
from botocore.client import BaseClient

from . import clients, core
from .clients import BOTO_CONFIG_RETRYABLE

# --- 1. SETUP: Configuration, Validation, and Clients ---

def get_env_var(name: str, default: Optional[str] = None) -> str:
    """
    Gets an environment variable or raises a ValueError for fast-failure.

    Args:
        name: The name of the environment variable.
        default: An optional default value. If not provided, the variable is required.

    Returns:
        The value of the environment variable.

    Raises:
        ValueError: If the required environment variable is not set.
    """
    value = os.environ.get(name, default)
    if value is None:
        raise ValueError(f"FATAL: Environment variable '{name}' is not set.")
    return value

# --- Configuration (loaded once at cold start) ---
LANDING_BUCKET = get_env_var("LANDING_BUCKET")
MINIO_SECRET_ID = get_env_var("MINIO_SECRET_ID")
MINIO_BUCKET = get_env_var("MINIO_BUCKET")
QUEUE_URL = get_env_var("QUEUE_URL")
IDEMPOTENCY_TABLE = get_env_var("IDEMPOTENCY_TABLE")
ENVIRONMENT = get_env_var("ENVIRONMENT", "dev")
LOG_LEVEL = get_env_var("LOG_LEVEL", "INFO").upper()
IDEMPOTENCY_TTL_HOURS = int(get_env_var("IDEMPOTENCY_TTL_HOURS", "24"))
SECRET_CACHE_TTL_SECONDS = int(get_env_var("SECRET_CACHE_TTL_SECONDS", "300"))
MINIO_SSE_TYPE = get_env_var("MINIO_SSE_TYPE", "AES256")
# Default raised to 32 to better handle 100 files/sec burst requirement (NFR-05).
MAX_FETCH_WORKERS = int(get_env_var("MAX_FETCH_WORKERS", "32"))
QUEUE_DEPTH_FACTOR = int(get_env_var("QUEUE_DEPTH_FACTOR", "4"))
SPOOL_MAX_MEMORY_BYTES = int(get_env_var("SPOOL_MAX_MEMORY_BYTES", "104857600"))
ARCHIVE_TIMEOUT_SECONDS = int(get_env_var("ARCHIVE_TIMEOUT_SECONDS", "300"))
# Default raised to 5s to be more resilient to transient writer slowdowns (NFR-02).
QUEUE_PUT_TIMEOUT_SECONDS = int(get_env_var("QUEUE_PUT_TIMEOUT_SECONDS", "5"))

# --- Global Setup ---
logger = logging.getLogger()
logger.setLevel(LOG_LEVEL)

S3, SQS, DDB, SECRETS = clients.get_boto_clients()

MINIO: Optional[BaseClient] = None
MINIO_SECRET_CACHE = {"data": None, "timestamp": datetime.min.replace(tzinfo=timezone.utc)}

# --- 2. STATEFUL & ORCHESTRATION LOGIC ---

class HashingStreamWrapper:
    """Wraps a file-like object to compute a SHA256 hash on the fly as it is being read."""
    def __init__(self, stream):
        self._stream = stream
        self._hasher = hashlib.sha256()

    def read(self, size=-1):
        chunk = self._stream.read(size)
        if chunk: self._hasher.update(chunk)
        return chunk

    def hexdigest(self):
        return self._hasher.hexdigest()

def get_minio_client(force_refresh: bool = False) -> BaseClient:
    """
    Retrieves a MinIO S3 client, using a time-based cache for credentials.

    This function manages the stateful, cached MinIO client. It's kept in this
    module because its lifecycle is tied to the Lambda execution environment.

    Args:
        force_refresh: If True, bypasses the cache and fetches new credentials.
                       Used for retrying after an authentication failure.

    Returns:
        A boto3 S3 client configured for the MinIO endpoint.

    Raises:
        botocore.exceptions.ClientError: If retrieving the secret from Secrets Manager fails.
    """
    global MINIO, MINIO_SECRET_CACHE
    now = datetime.now(timezone.utc)
    cache_expiry = MINIO_SECRET_CACHE["timestamp"] + timedelta(seconds=SECRET_CACHE_TTL_SECONDS)
    if MINIO and not force_refresh and now < cache_expiry:
        return MINIO

    logger.info(f"Refreshing MinIO client credentials. Force refresh: {force_refresh}")
    secret_value = SECRETS.get_secret_value(SecretId=MINIO_SECRET_ID)
    secret_data = json.loads(secret_value["SecretString"])
    MINIO_SECRET_CACHE = {"data": secret_data, "timestamp": now}

    MINIO = boto3.client("s3", endpoint_url=secret_data["endpoint_url"], aws_access_key_id=secret_data["access_key"], aws_secret_access_key=secret_data["secret_key"], config=BOTO_CONFIG_RETRYABLE)
    return MINIO

def stream_archive_to_minio(s3_keys: list[str], dest_key: str) -> str:
    """
    Streams files from S3, creates a tar.gz archive in-memory, uploads to MinIO,
    and verifies the integrity of the uploaded object to satisfy REQ-05.

    This function uses a thread-safe producer-consumer pattern to achieve parallelism.
    - Producers (fetchers): A thread pool fetches S3 objects concurrently.
    - Consumer (writer): A single thread writes objects to the tar archive.

    To prevent deadlocks from back-pressure, producers use a short timeout when
    putting items onto the queue. If the queue is full, they inject an error and
    stop, causing the entire batch to fail fast and be retried by SQS.
    A sentinel value (`None`) is used to signal completion.

    The integrity check is a two-pass process on the in-memory archive:
    1. The entire archive is built and its SHA256 checksum is computed.
    2. The archive is streamed again for upload, with the checksum attached as
       metadata. A final `head_object` call verifies this metadata on the
       object in MinIO.

    Args:
        s3_keys: A list of S3 object keys to include in the archive.
        dest_key: The destination key for the archive in the MinIO bucket.

    Returns:
        The hex digest of the SHA256 checksum of the created archive.
    """
    data_queue: queue.Queue = queue.Queue(maxsize=MAX_FETCH_WORKERS * QUEUE_DEPTH_FACTOR)
    error_queue: queue.Queue = queue.Queue()

    def _fetcher(key: str):
        try:
            s3_obj = S3.get_object(Bucket=LANDING_BUCKET, Key=key)
            data_queue.put((key, s3_obj["Body"], s3_obj["ContentLength"]), timeout=QUEUE_PUT_TIMEOUT_SECONDS)
        except queue.Full:
            err = RuntimeError(f"Queue full while fetching {key}; writer may be stalled.")
            error_queue.put(err)
            logger.error(err)
        except Exception as e:
            logger.exception(f"Fetcher thread failed for key {key}")
            error_queue.put(e)

    def _writer(spooled_file: tempfile.SpooledTemporaryFile):
        try:
            with tarfile.open(fileobj=spooled_file, mode="w:gz") as tar:
                while True:
                    item = data_queue.get()
                    if item is None: break
                    key, body, size = item
                    with body:
                        tarinfo = tarfile.TarInfo(name=key)
                        tarinfo.size = size
                        tar.addfile(tarinfo, body)
        except Exception as e:
            logger.exception("Writer thread failed")
            error_queue.put(e)

    with tempfile.SpooledTemporaryFile(max_size=SPOOL_MAX_MEMORY_BYTES, mode='w+b') as spooled_archive:
        writer_thread = threading.Thread(target=_writer, args=(spooled_archive,))
        writer_thread.start()
        with ThreadPoolExecutor(max_workers=MAX_FETCH_WORKERS) as executor:
            for s3_key in s3_keys:
                executor.submit(_fetcher, s3_key)
        data_queue.put(None)
        writer_thread.join(timeout=ARCHIVE_TIMEOUT_SECONDS)

        if not error_queue.empty(): raise error_queue.get()
        if writer_thread.is_alive(): raise TimeoutError("Archive writer thread timed out.")

        if getattr(spooled_archive, '_rolled', False):
             logger.warning(f"Archive size exceeded {SPOOL_MAX_MEMORY_BYTES} bytes; spooled to disk.")
             core.emit_metrics(ENVIRONMENT, "Info", {"ArchiveSpilled": 1})

        # --- Data Integrity Check (REQ-05) ---
        # Pass 1: Calculate the SHA256 checksum of the completed archive.
        spooled_archive.seek(0)
        sha256 = hashlib.sha256()
        while chunk := spooled_archive.read(8192):
            sha256.update(chunk)
        digest = sha256.hexdigest()

        # Pass 2: Upload the file with the checksum as metadata.
        spooled_archive.seek(0)
        minio_client = get_minio_client()
        extra_args = {
            "ServerSideEncryption": MINIO_SSE_TYPE if MINIO_SSE_TYPE != "NONE" else None,
            "Metadata": {"sha256_checksum": digest}
        }
        minio_client.upload_fileobj(spooled_archive, MINIO_BUCKET, dest_key, ExtraArgs={k: v for k, v in extra_args.items() if v is not None})

        # Verification step: Ensure the object in MinIO has the correct checksum.
        head = minio_client.head_object(Bucket=MINIO_BUCKET, Key=dest_key)
        remote_checksum = head.get("Metadata", {}).get("sha256_checksum")
        if remote_checksum != digest:
            raise RuntimeError(f"Checksum mismatch for {dest_key}: local={digest}, remote={remote_checksum}")

        logger.info(f"Successfully uploaded and verified checksum for {dest_key}")
        return digest

def _build_response(status_code: int, body: Dict[str, Any]) -> Dict[str, Any]:
    """Centralized helper to build the final Lambda response."""
    return {"statusCode": status_code, "body": json.dumps(body)}

# --- 3. LAMBDA HANDLER ---

def handler(event: Dict, context: Any):
    """
    Main Lambda entry point. Orchestrates the entire aggregation process.

    This function is triggered by a batch of SQS messages and returns a
    dictionary for logging and unit testing convenience. It does not integrate
    with API Gateway. It will re-raise exceptions to let SQS handle retries.

    This function follows these steps:
    1. Fetches current queue depth and emits a metric (NFR-07).
    2. Receives a batch of SQS messages.
    3. Calls `core.filter_unique_objects` to perform idempotency checks.
    4. Deletes SQS messages corresponding to duplicate files.
    5. If new files exist, calls `stream_archive_to_minio` to process them.
    6. On successful upload, deletes SQS messages for the processed files.
    7. Emits success or failure metrics.
    """
    start_time = datetime.now(timezone.utc)
    sqs_messages = event.get("Records", [])
    if not sqs_messages:
        return _build_response(200, {"message": "No messages to process."})

    logger.info(f"Received {len(sqs_messages)} messages to process.")

    try:
        # Emit queue depth metric for observability (NFR-07).
        try:
            attrs = SQS.get_queue_attributes(QueueUrl=QUEUE_URL, AttributeNames=["ApproximateNumberOfMessages"])
            queue_depth = int(attrs.get("Attributes", {}).get("ApproximateNumberOfMessages", 0))
            core.emit_metrics(ENVIRONMENT, "Info", {"QueueDepth": queue_depth})
        except Exception as e:
            logger.warning(f"Could not get queue attributes: {e}")

        table = DDB.Table(IDEMPOTENCY_TABLE)
        ttl = int((datetime.now(timezone.utc) + timedelta(hours=IDEMPOTENCY_TTL_HOURS)).timestamp())

        filter_result = core.filter_unique_objects(table, ttl, sqs_messages)
        core.delete_sqs_messages(SQS, QUEUE_URL, filter_result.messages_to_delete_as_duplicates)

        if not filter_result.unique_keys:
            logger.info("All messages in this batch were duplicates. Exiting.")
            return _build_response(200, {"message": "All messages were duplicates."})

        dest_key = f"archive/{start_time.strftime('%Y/%m/%d/%H%M%S')}-{context.aws_request_id}.tar.gz"
        digest = stream_archive_to_minio(filter_result.unique_keys, dest_key)

        total_delete_failures = core.delete_sqs_messages(SQS, QUEUE_URL, filter_result.messages_to_process)
        if total_delete_failures > 0:
            logger.error(f"{total_delete_failures} messages could not be deleted after successful processing.")

        latency_ms = int((datetime.now(timezone.utc) - start_time).total_seconds() * 1000)
        log_payload = {"output_key": dest_key, "source_files": len(filter_result.unique_keys), "duplicates_skipped": filter_result.duplicates_found, "delete_failures": total_delete_failures, "sha256_checksum": digest, "latency_ms": latency_ms}

        core.emit_metrics(ENVIRONMENT, "Success", log_payload)
        logger.info(f"Successfully processed batch: {log_payload['output_key']}")
        return _build_response(200, log_payload)

    except Exception as e:
        latency_ms = int((datetime.now(timezone.utc) - start_time).total_seconds() * 1000)
        error_payload = {"error_type": type(e).__name__, "error_message": str(e), "latency_ms": latency_ms}
        core.emit_metrics(ENVIRONMENT, "Failure", error_payload)
        logger.error(f"Processing failed: {json.dumps(error_payload)}", exc_info=True)
        raise