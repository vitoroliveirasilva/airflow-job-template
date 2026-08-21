"""Small helpers for consistent, secret-conscious structured log lines"""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from datetime import date
from typing import Any

from airflow_job_template._security import contains_sensitive_uri_data, is_sensitive_field_name
from airflow_job_template.runtime.context import JobRunContext


def _safe_value(value: Any) -> str | int | float | bool | None:
    if value is None or isinstance(value, int | float | bool):
        return value
    if isinstance(value, str):
        return "<redacted>" if contains_sensitive_uri_data(value) else value
    if isinstance(value, date):
        return value.isoformat()
    return f"<{type(value).__name__}>"


def redact_fields(fields: Mapping[str, Any]) -> dict[str, Any]:
    """Redact sensitive names and avoid serializing arbitrary object representations"""

    return {
        key: "<redacted>" if is_sensitive_field_name(key) else _safe_value(value)
        for key, value in fields.items()
    }


def log_event(
    logger: logging.Logger,
    event: str,
    *,
    context: JobRunContext | None = None,
    level: int = logging.INFO,
    **fields: Any,
) -> None:
    """Emit one compact JSON object through normal Python/Airflow logging"""

    if not isinstance(event, str) or not event.strip():
        raise ValueError("event must be a non-blank string")

    payload: dict[str, Any] = {"event": event}
    if context is not None:
        payload.update(
            {
                "dag_id": context.dag_id,
                "task_id": context.task_id,
                "run_id": context.run_id,
                "try_number": context.try_number,
            }
        )
    payload.update(redact_fields(fields))
    logger.log(level, "%s", json.dumps(payload, sort_keys=True, separators=(",", ":")))
