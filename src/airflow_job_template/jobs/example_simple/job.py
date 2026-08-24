"""Minimal pure-Python job logic"""

import logging

from airflow_job_template.observability import log_event
from airflow_job_template.runtime import JobResult, JobRunContext


def run(context: JobRunContext) -> JobResult:
    """Process one deterministic demonstration unit without external dependencies"""

    logger = logging.getLogger(__name__)
    log_event(logger, "records_validated", context=context, processed=1)
    return JobResult(processed=1, created=0, updated=0, skipped=0)
