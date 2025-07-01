# aggregator_lambda/model.py

from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass
class SQSMessageState:
    """Tracks the state of an SQS message as its S3 records are evaluated."""

    msg: Dict[str, Any]
    total_records: int
    has_new_key: bool = False


@dataclass
class FilterResult:
    """Clear data structure for the output of the filtering logic."""

    unique_keys: List[str] = field(default_factory=list)
    messages_to_process: List[Dict] = field(default_factory=list)
    messages_to_delete_as_duplicates: List[Dict] = field(default_factory=list)
    duplicates_found: int = 0
