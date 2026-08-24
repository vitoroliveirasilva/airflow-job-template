"""RPA transaction pattern without importing Selenium/Playwright in the core environment"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Protocol

from airflow_job_template.observability import log_event
from airflow_job_template.runtime import (
    JobResult,
    JobRunContext,
    NonRetryableJobError,
    RetryableJobError,
)

_ALREADY_UPDATED_STATE = "updated"
_MUTABLE_STATE = "pending"


class PortalSession(Protocol):
    def current_state(self, record_id: str) -> str: ...
    def apply_update(self, record_id: str) -> None: ...
    def validate_update(self, record_id: str) -> bool: ...
    def screenshot(self, path: Path) -> None: ...
    def close(self) -> None: ...


def update_record(
    context: JobRunContext,
    *,
    record_id: str,
    session: PortalSession,
    diagnostic_path: Path,
) -> JobResult:
    """Check state before mutation, preserve primary failures, and always clean up"""

    logger = logging.getLogger(__name__)
    failed = False
    diagnostic_capture_allowed = False
    try:
        if not isinstance(record_id, str) or not record_id.strip():
            raise NonRetryableJobError("record_id must be a non-blank string")
        if record_id != record_id.strip():
            raise NonRetryableJobError("record_id must not contain surrounding whitespace")
        if not isinstance(diagnostic_path, Path):
            raise NonRetryableJobError("diagnostic_path must be a pathlib.Path")
        diagnostic_capture_allowed = True

        state = session.current_state(record_id)
        if not isinstance(state, str):
            raise NonRetryableJobError("portal returned a non-string state")
        if state == _ALREADY_UPDATED_STATE:
            return JobResult(processed=1, skipped=1, batch_id=context.run_id)
        if state != _MUTABLE_STATE:
            raise NonRetryableJobError("refusing RPA mutation from unexpected portal state")

        session.apply_update(record_id)
        validation_result = session.validate_update(record_id)
        if not isinstance(validation_result, bool):
            raise NonRetryableJobError("portal returned a non-boolean validation result")
        if not validation_result:
            # Retry is safe because every attempt starts by checking whether the mutation landed
            raise RetryableJobError("portal did not confirm the update")
        return JobResult(processed=1, updated=1, batch_id=context.run_id)
    except Exception:
        failed = True
        if diagnostic_capture_allowed:
            try:
                # Store diagnostics only in a deployment-appropriate location. Never capture
                # screens containing secrets/PII. This path is supplied by the caller for that
                # reason.
                session.screenshot(diagnostic_path)
            except Exception:
                log_event(
                    logger,
                    "rpa_diagnostic_capture_failed",
                    context=context,
                    level=logging.WARNING,
                )
        raise
    finally:
        try:
            session.close()
        except Exception:
            # A cleanup failure after a confirmed side effect must not turn success into a retry
            log_event(
                logger,
                "rpa_session_cleanup_failed",
                context=context,
                level=logging.WARNING,
                preserving_primary_failure=failed,
            )
