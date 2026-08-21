"""Small helpers for consistent, secret-conscious structured log lines"""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from datetime import date, datetime
from typing import Any

from airflow_job_template.runtime.context import JobRunContext

_SENSITIVE_FRAGMENTS = (
    "authorization",
    "cookie",
    "credential",
    "password",
    "secret",
    "token",
)


def _is_sensitive_key(key: str) -> bool:
    lowered = key.lower()
    return any(fragment in lowered for fragment in _SENSITIVE_FRAGMENTS)


def _safe_value(value: Any) -> str | int | float | bool | None:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return str(value)


def redact_fields(fields: Mapping[str, Any]) -> dict[str, Any]:
    """Redact fields with obviously sensitive names before they reach the logger"""

    return {
        key: "<redacted>" if _is_sensitive_key(key) else _safe_value(value)
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

    if not event or not event.strip():
        raise ValueError("event cannot be blank")

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
