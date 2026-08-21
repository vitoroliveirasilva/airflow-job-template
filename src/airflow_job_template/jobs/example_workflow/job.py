"""Pure business functions for the explicit multi-task example"""

from typing import Any

from airflow_job_template.runtime import JobConfigurationError


def extract(limit: int) -> dict[str, Any]:
    if not 1 <= limit <= 10_000:
        raise JobConfigurationError("limit must be between 1 and 10000")
    return {"batch_id": f"example-{limit}", "record_count": limit}


def validate(metadata: dict[str, Any]) -> dict[str, Any]:
    count = metadata.get("record_count")
    batch_id = metadata.get("batch_id")
    if not isinstance(count, int) or count < 0 or not isinstance(batch_id, str):
        raise JobConfigurationError("invalid extract metadata")
    return {"batch_id": batch_id, "record_count": count, "validated": True}


def load(metadata: dict[str, Any]) -> dict[str, Any]:
    if metadata.get("validated") is not True:
        raise JobConfigurationError("load requires validated metadata")
    return {
        "batch_id": metadata["batch_id"],
        "processed": metadata["record_count"],
    }
