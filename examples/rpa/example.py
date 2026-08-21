"""RPA transaction pattern without importing Selenium/Playwright in the core environment"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Protocol

from airflow_job_template.observability import log_event
from airflow_job_template.runtime import JobResult, JobRunContext


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
    try:
        if session.current_state(record_id) == "updated":
            return JobResult(processed=1, skipped=1, batch_id=context.run_id)
        session.apply_update(record_id)
        if not session.validate_update(record_id):
            raise RuntimeError("portal did not confirm the update")
        return JobResult(processed=1, updated=1, batch_id=context.run_id)
    except Exception:
        failed = True
        try:
            # Store diagnostics only in a deployment-appropriate location. Never capture screens
            # containing secrets/PII. This path is supplied by the caller for that reason.
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
            if not failed:
                raise
            log_event(logger, "rpa_session_cleanup_failed", context=context, level=logging.WARNING)
