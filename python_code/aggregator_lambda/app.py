"""
Main AWS Lambda handler for the Data Aggregation Pipeline.

This module has been refactored to use the AWS Lambda Powertools toolkit, which
provides best-practice utilities for logging, metrics, tracing, and secrets
management. This results in simpler, more maintainable, and more observable code.
"""

import hashlib
import json
import os
import queue
import tarfile
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

import boto3

# --- Powertools Setup ---
from aws_lambda_powertools import Logger, Metrics, Tracer
from aws_lambda_powertools.logging import correlation_paths
from aws_lambda_powertools.metrics import MetricUnit
from aws_lambda_powertools.utilities.parameters import SecretsProvider
from botocore.client import BaseClient
from botocore.exceptions import ClientError

from . import clients, core
from .clients import BOTO_CONFIG_RETRYABLE


# --- 1. SETUP: Configuration and Powertools Initialization ---


def get_env_var(name: str, default: Optional[str] = None) -> str:
    """Gets an environment variable or raises a ValueError for fast-failure."""
    value = os.environ.get(name, default)
    if value is None:
        raise ValueError(f"FATAL: Environment variable '{name}' is not set.")
    return value


# --- Configuration ---
POWERTOOLS_SERVICE_NAME = get_env_var("POWERTOOLS_SERVICE_NAME", "DataAggregator")
POWERTOOLS_METRICS_NAMESPACE = get_env_var(
    "POWERTOOLS_METRICS_NAMESPACE", "DataMovePipeline"
)
LANDING_BUCKET = get_env_var("LANDING_BUCKET")
MINIO_SECRET_ID = get_env_var("MINIO_SECRET_ID")
MINIO_BUCKET = get_env_var("MINIO_BUCKET")
QUEUE_URL = get_env_var("QUEUE_URL")
IDEMPOTENCY_TABLE = get_env_var("IDEMPOTENCY_TABLE")
ENVIRONMENT = get_env_var("ENVIRONMENT", "dev")
MINIO_SSE_TYPE = get_env_var("MINIO_SSE_TYPE", "AES256")
IDEMPOTENCY_TTL_HOURS = int(get_env_var("IDEMPOTENCY_TTL_HOURS", "24"))
SECRET_CACHE_TTL_SECONDS = int(get_env_var("SECRET_CACHE_TTL_SECONDS", "300"))
MAX_FETCH_WORKERS = int(
    get_env_var("MAX_FETCH_WORKERS", "8")
)  # 8 or 16 probably best for a 1-2 vCPU
SPOOL_MAX_MEMORY_BYTES = int(get_env_var("SPOOL_MAX_MEMORY_BYTES", "268435456"))
ARCHIVE_TIMEOUT_SECONDS = int(get_env_var("ARCHIVE_TIMEOUT_SECONDS", "300"))
QUEUE_PUT_TIMEOUT_SECONDS = int(get_env_var("QUEUE_PUT_TIMEOUT_SECONDS", "5"))
MIN_REMAINING_TIME_MS = int(get_env_var("MIN_REMAINING_TIME_MS", "60000"))
MAX_FILE_SIZE_BYTES = int(get_env_var("MAX_FILE_SIZE_BYTES", "5242880"))

# --- Powertools and Boto3 Client Initialization ---
logger = Logger(service=POWERTOOLS_SERVICE_NAME)
metrics = Metrics(namespace=POWERTOOLS_METRICS_NAMESPACE)
tracer = Tracer(service=POWERTOOLS_SERVICE_NAME)
secrets_provider = SecretsProvider()
S3, SQS, DDB, _ = clients.get_boto_clients()
_minio_client_creation_lock = threading.Lock()
_MINIO_CLIENT: Optional[BaseClient] = None

# --- 2. STATEFUL & ORCHESTRATION LOGIC ---


class ArchiveHasher:
    """Wraps a file-like object to compute a SHA256 hash on the fly as it is being read."""

    def __init__(self, stream):
        self._stream = stream
        self._hasher = hashlib.sha256()

    def read(self, size=-1):
        chunk = self._stream.read(size)
        if chunk:
            self._hasher.update(chunk)
        return chunk

    def hexdigest(self):
        return self._hasher.hexdigest()


@tracer.capture_method
def get_minio_client() -> BaseClient:
    """Retrieves a MinIO S3 client, caching the client object for performance."""
    global _MINIO_CLIENT
    if _MINIO_CLIENT:
        return _MINIO_CLIENT

    with _minio_client_creation_lock:
        if _MINIO_CLIENT:
            return _MINIO_CLIENT

        logger.info("Creating new MinIO client instance.")
        secret_data: dict = secrets_provider.get(
            MINIO_SECRET_ID,
            max_age=SECRET_CACHE_TTL_SECONDS,
            transform="json",  # This parses the JSON secret string into a dict
        )
        _MINIO_CLIENT = boto3.client(
            "s3",
            endpoint_url=secret_data["endpoint_url"],
            aws_access_key_id=secret_data["access_key"],
            aws_secret_access_key=secret_data["secret_key"],
            config=BOTO_CONFIG_RETRYABLE,
        )
        return _MINIO_CLIENT


@tracer.capture_method
def stream_archive_to_minio(s3_keys: list[str], dest_key: str) -> str:
    """
    Streams files from S3, archives them, and securely uploads to MinIO.

    This function orchestrates a high-throughput, memory-efficient archiving
    process using a multi-threaded producer-consumer pattern.

    - Producer Threads (`_fetcher`): A pool of worker threads fetches S3 object
      metadata and their corresponding data streams concurrently.
    - Consumer Thread (`_writer`): A single thread consumes the data streams
      and writes them into a single gzipped tarball (`.tar.gz`).

    The archive is initially created in memory using `SpooledTemporaryFile`. If
    its size exceeds a configured threshold (`SPOOL_MAX_MEMORY_BYTES`), it
    automatically spills over to the Lambda's ephemeral `/tmp` disk storage to
    prevent memory exhaustion.

    Finally, the completed archive is uploaded to MinIO. The function computes a
    SHA256 checksum during the upload, attaches it as metadata, and performs a
    final verification step to ensure end-to-end data integrity.

    Args:
        s3_keys: A list of S3 object keys to be included in the archive.
        dest_key: The destination object key for the final archive in MinIO.

    Returns:
        The hex-encoded SHA256 checksum of the successfully uploaded archive.

    Raises:
        RuntimeError: If any thread encounters a fatal error, the upload or
                      verification fails, or the archive exceeds /tmp limits.
        TimeoutError: If the writer thread fails to complete within its timeout.
    """
    data_queue: queue.Queue = queue.Queue(maxsize=MAX_FETCH_WORKERS * 4)
    error_queue: queue.Queue = queue.Queue()
    error_event = threading.Event()

    @tracer.capture_method
    def _fetcher(key: str):
        """
        Producer thread: Fetches an S3 object's stream and passes it to the queue.

        This function is responsible for cleaning up the S3 connection ONLY if
        it fails to hand off the stream to the writer thread.
        """
        s3_obj = None
        try:
            s3_obj = S3.get_object(Bucket=LANDING_BUCKET, Key=key)
            content_length = s3_obj["ContentLength"]
            if content_length > MAX_FILE_SIZE_BYTES:
                raise ValueError(
                    f"File {key} ({content_length} bytes) exceeds max size."
                )

            data_queue.put(
                (key, s3_obj["Body"], content_length),
                timeout=QUEUE_PUT_TIMEOUT_SECONDS
            )

        except queue.Full:
            # This is a specific, important failure mode indicating back-pressure.
            err = RuntimeError("Queue full; writer thread may be stalled or too slow.")
            logger.warning(
                "Back-pressure detected in fetcher thread.",
                extra={"error": str(err), "key": key}
            )
            # Emit a metric to make this event observable and alarm-able.
            metrics.add_metric(name="QueuePutStalled", unit=MetricUnit.Count, value=1)

            # Now handle cleanup and error propagation.
            if s3_obj:
                try:
                    s3_obj["Body"].close()
                except Exception as close_exc:
                    logger.warning("Failed to close S3 stream during queue.Full handling.",
                                   extra={"close_error": str(close_exc)})

            error_queue.put(err)
            error_event.set()

        except Exception as e:
            # Catch all other exceptions (e.g., from boto3, ValueError).
            if s3_obj:
                try:
                    s3_obj["Body"].close()
                except Exception as close_exc:
                    logger.warning("Failed to close S3 stream during generic error handling.",
                                   extra={"close_error": str(close_exc)})

            logger.exception(f"Fetcher thread failed for key {key}")
            error_queue.put(e)
            error_event.set()

    @tracer.capture_method
    def _writer(spooled_file: tempfile.SpooledTemporaryFile):
        """
        Consumer thread: Writes streams into a tar archive and closes them.

        This function is responsible for closing each S3 stream after it has
        been successfully written to the archive.
        """
        try:
            with tarfile.open(fileobj=spooled_file, mode="w:gz") as tar:
                while not error_event.is_set():
                    try:
                        item = data_queue.get(block=True, timeout=0.1)
                        if item is None:
                            break

                        key, body_stream, size = item

                        try:
                            tarinfo = tarfile.TarInfo(name=key)
                            tarinfo.size = size
                            tar.addfile(tarinfo, body_stream)
                        finally:
                            # CRITICAL: Always close the stream after using it,
                            # whether tar.addfile succeeded or failed.
                            body_stream.close()

                    except queue.Empty:
                        continue
        except Exception as e:
            logger.exception("Writer thread failed")
            error_queue.put(e)
            error_event.set()

    # Use a spooled file that lives in memory until it hits the max size,
    # at which point it spills to disk transparently.
    with tempfile.SpooledTemporaryFile(
        max_size=SPOOL_MAX_MEMORY_BYTES, mode="w+b"
    ) as spooled_archive:
        writer_thread = threading.Thread(
            name="tar-writer", target=_writer, args=(spooled_archive,)
        )
        writer_thread.start()

        # Start the producer threads to fetch files concurrently.
        with ThreadPoolExecutor(max_workers=MAX_FETCH_WORKERS) as executor:
            for s3_key in s3_keys:
                if error_event.is_set():
                    break
                executor.submit(_fetcher, s3_key)

        # Once all fetchers are submitted, signal to the writer that no more
        # files are coming.
        if not error_event.is_set():
            data_queue.put(None)

        writer_thread.join(timeout=ARCHIVE_TIMEOUT_SECONDS)

        # Centralized error checking after threads complete.
        if not error_queue.empty():
            raise error_queue.get()
        if writer_thread.is_alive():
            raise TimeoutError("Archive writer thread timed out.")

        # Explicitly flush any final data from internal buffers to the file.
        spooled_archive.flush()

        # Check if the archive spilled to disk and enforce a safety limit.
        is_on_disk = hasattr(spooled_archive, "name") and os.path.exists(
            spooled_archive.name
        )
        if is_on_disk:
            archive_size_on_disk = os.fstat(spooled_archive.fileno()).st_size
            logger.warning(
                "Archive spooled to disk", extra={"size_bytes": archive_size_on_disk}
            )
            metrics.add_metric(
                name="ArchiveSpilledToDisk", unit=MetricUnit.Count, value=1
            )
            if archive_size_on_disk > 400 * 1024 * 1024:  # 400 MB guardrail
                raise MemoryError(
                    f"Archive on disk ({archive_size_on_disk} bytes) exceeds safe /tmp limit."
                )

        # Get final archive size for metrics before read operations begin.
        archive_size_bytes = spooled_archive.tell()
        metrics.add_metric(
            name="ArchiveSizeBytes", unit=MetricUnit.Bytes, value=archive_size_bytes
        )

        # --- Upload and Verify ---
        for attempt in range(2):
            try:
                minio_client = get_minio_client()

                # Rewind the archive to the beginning for the read pass (upload).
                spooled_archive.seek(0)

                # The hasher wraps the file stream to compute the checksum on the fly.
                hasher = ArchiveHasher(spooled_archive)

                extra_args = {}
                if MINIO_SSE_TYPE != "NONE":
                    extra_args["ServerSideEncryption"] = MINIO_SSE_TYPE

                minio_client.upload_fileobj(
                    hasher, MINIO_BUCKET, dest_key, ExtraArgs=extra_args
                )
                digest = hasher.hexdigest()

                # To attach the checksum as metadata, we must perform a self-copy.
                minio_client.copy_object(
                    Bucket=MINIO_BUCKET,
                    Key=dest_key,
                    CopySource={"Bucket": MINIO_BUCKET, "Key": dest_key},
                    Metadata={"sha256_checksum": digest},
                    MetadataDirective="REPLACE",
                    **extra_args,
                )

                # Final integrity check: read the metadata back and verify the checksum.
                head = minio_client.head_object(Bucket=MINIO_BUCKET, Key=dest_key)
                remote_checksum = head.get("Metadata", {}).get("sha256_checksum")
                if remote_checksum != digest:
                    raise RuntimeError(
                        "Data integrity failure: checksum mismatch after upload."
                    )

                logger.info("Successfully uploaded and verified archive.")
                return digest

            except ClientError as e:
                # If we get an auth error on the first try, our cached credentials
                # may be stale. Invalidate caches and retry once.
                if (
                    e.response["Error"]["Code"]
                    in ["AccessDenied", "InvalidAccessKeyId"]
                    and attempt == 0
                ):
                    logger.warning(
                        "MinIO access denied. Invalidating secret and client caches and retrying."
                    )
                    global _MINIO_CLIENT
                    secrets_provider.clear_cache()
                    _MINIO_CLIENT = None
                    continue
                raise

    # This line is reached only if the upload loop fails after all attempts.
    raise RuntimeError("Failed to upload archive to MinIO after all attempts.")


def _build_response(status_code: int, body: Dict[str, Any]) -> Dict[str, Any]:
    return {"statusCode": status_code, "body": json.dumps(body)}


# --- 3. LAMBDA HANDLER ---


@tracer.capture_lambda_handler
@logger.inject_lambda_context(correlation_id_path=correlation_paths.SQS, log_event=True)
@metrics.log_metrics(capture_cold_start_metric=True)
def handler(event: Dict, context: Any):
    """Main Lambda entry point. Orchestrates the entire aggregation process."""
    metrics.add_dimension(name="Environment", value=ENVIRONMENT)
    start_time = datetime.now(timezone.utc)

    sqs_messages = event.get("Records", [])
    if not sqs_messages:
        metrics.flush_metrics()
        return _build_response(200, {"message": "No messages to process."})

    try:
        # Fetch both visible (queued) and not-visible (in-flight) message counts
        attrs = SQS.get_queue_attributes(
            QueueUrl=QUEUE_URL,
            AttributeNames=[
                "ApproximateNumberOfMessages",
                "ApproximateNumberOfMessagesNotVisible",
            ],
        )
        attributes = attrs.get("Attributes", {})

        # Metric for messages waiting in the queue
        metrics.add_metric(
            name="QueueDepth",
            unit=MetricUnit.Count,
            value=int(attributes.get("ApproximateNumberOfMessages", 0)),
        )

        # CRITICAL METRIC: Messages currently being processed by Lambda instances.
        # An alert on this metric can indicate when the pipeline is at capacity.
        metrics.add_metric(
            name="InFlightMessages",
            unit=MetricUnit.Count,
            value=int(attributes.get("ApproximateNumberOfMessagesNotVisible", 0)),
        )

    except Exception as e:
        logger.warning(f"Could not get queue attributes: {e}")

    if context.get_remaining_time_in_millis() < MIN_REMAINING_TIME_MS:
        raise TimeoutError("Not enough time remaining to process batch; re-queuing.")

    table = DDB.Table(IDEMPOTENCY_TABLE)
    ttl = int(
        (
            datetime.now(timezone.utc) + timedelta(hours=IDEMPOTENCY_TTL_HOURS)
        ).timestamp()
    )
    filter_result = core.filter_unique_objects(table, ttl, sqs_messages, logger)

    core.delete_sqs_messages(
        SQS, QUEUE_URL, filter_result.messages_to_delete_as_duplicates, logger
    )

    if not filter_result.unique_keys:
        logger.info("All messages in this batch were duplicates. Exiting.")
        metrics.add_metric(
            name="DuplicateFilesSkipped",
            unit=MetricUnit.Count,
            value=filter_result.duplicates_found,
        )
        metrics.flush_metrics()
        return _build_response(200, {"message": "All messages were duplicates."})

    dest_key = f"archive/{datetime.now(timezone.utc).strftime('%Y/%m/%d/%H%M%S')}-{context.aws_request_id}.tar.gz"
    digest = stream_archive_to_minio(filter_result.unique_keys, dest_key)

    delete_failures = core.delete_sqs_messages(
        SQS, QUEUE_URL, filter_result.messages_to_process, logger
    )
    latency_ms = int((datetime.now(timezone.utc) - start_time).total_seconds() * 1000)

    logger.append_keys(output_key=dest_key, sha256_checksum=digest)
    metrics.add_metric(
        name="ProcessingLatency", unit=MetricUnit.Milliseconds, value=latency_ms
    )
    metrics.add_metric(
        name="FilesProcessed",
        unit=MetricUnit.Count,
        value=len(filter_result.unique_keys),
    )
    metrics.add_metric(
        name="DuplicateFilesSkipped",
        unit=MetricUnit.Count,
        value=filter_result.duplicates_found,
    )
    metrics.add_metric(
        name="SQSDeleteFailures", unit=MetricUnit.Count, value=delete_failures
    )

    logger.info("Successfully processed batch.")
    return _build_response(
        200, {"output_key": dest_key, "source_files": len(filter_result.unique_keys)}
    )
