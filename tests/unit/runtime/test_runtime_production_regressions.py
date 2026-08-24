from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from airflow_job_template.runtime import (
    JobConfigurationError,
    JobResult,
    job_run_context_from_airflow,
)


def test_airflow_context_adapter_rejects_non_mapping_input() -> None:
    with pytest.raises(JobConfigurationError, match="must be a mapping"):
        job_run_context_from_airflow(None)


def test_runtime_context_rejects_naive_dates(job_context) -> None:
    with pytest.raises(JobConfigurationError, match="timezone-aware"):
        replace(job_context, logical_date=datetime(2026, 8, 21, 10, 0))


def test_runtime_context_rejects_reversed_data_interval(job_context) -> None:
    start = datetime(2026, 8, 21, 12, 0, tzinfo=UTC)
    end = start - timedelta(hours=1)
    with pytest.raises(JobConfigurationError, match="must not be after"):
        replace(job_context, data_interval_start=start, data_interval_end=end)


def test_job_result_keeps_string_metadata_bounded() -> None:
    with pytest.raises(JobConfigurationError, match="artifact_uri"):
        JobResult(artifact_uri="s3://bucket/" + "a" * 4096)
    with pytest.raises(JobConfigurationError, match="batch_id"):
        JobResult(batch_id="b" * 257)
