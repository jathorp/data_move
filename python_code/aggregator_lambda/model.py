"""
Data models for the Data Aggregation Pipeline.

This module defines the core data structures used to pass information between
different parts of the application. Using dataclasses and TypedDicts ensures
data contracts are explicit, statically checked by mypy, and self-documenting.
"""

from dataclasses import dataclass, field
from typing import List, TypedDict


class SQSEventRecord(TypedDict):
    """
    Represents the structure of a single SQS message record from a Lambda event.

    This provides static type checking for message attributes, ensuring that any
    access to keys like 'messageId' or 'receiptHandle' is validated by mypy.
    """

    messageId: str
    receiptHandle: str
    body: str
    # Other SQS attributes are available but are not used by this application.


@dataclass
class SQSMessageState:
    """
    A state-tracking object for a single SQS message during the filtering process.

    This is used to determine if a message, which may contain multiple S3 event
    records, has at least one "new" file that needs to be processed.

    Attributes:
        msg: The original SQS message record, conforming to SQSEventRecord.
        total_records: The number of S3 records found in the message body.
        has_new_key: A flag that is set to True if any S3 key from this
                     message is found to be unique (not a duplicate).
    """

    msg: SQSEventRecord
    total_records: int
    has_new_key: bool = False


@dataclass
class FilterResult:
    """
    A clear data structure for returning the outcome of the idempotency filtering logic.

    This structure is critical for data loss prevention. It explicitly separates
    SQS messages that can be safely deleted (because they only contain duplicates)
    from messages that must be kept until their corresponding files are successfully
    processed and archived.

    Attributes:
        unique_keys: A list of S3 object keys that passed the idempotency check.
        messages_to_process: A list of SQS messages that contained at least one
                             unique key. These should only be deleted after the
                             archive is successfully uploaded.
        messages_to_delete_as_duplicates: A list of SQS messages where all
                                          contained keys were duplicates. These can
                                          be deleted immediately.
        duplicates_found: A count of the unique S3 keys that were found to be
                          duplicates. This count aligns with the "DuplicateFilesSkipped"
                          metric.
    """

    unique_keys: List[str] = field(default_factory=list)
    messages_to_process: List[SQSEventRecord] = field(default_factory=list)
    messages_to_delete_as_duplicates: List[SQSEventRecord] = field(default_factory=list)
    duplicates_found: int = 0
