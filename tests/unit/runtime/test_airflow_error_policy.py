import sys

import pytest

if sys.platform == "win32":
    pytest.skip(
        "Airflow runtime tests require a POSIX environment; use WSL2/Linux or CI",
        allow_module_level=True,
    )

pytest.importorskip("airflow")

from airflow.sdk.exceptions import AirflowFailException

from airflow_job_template.runtime import (
    JobConfigurationError,
    RetryableJobError,
    run_with_airflow_error_policy,
)


def test_non_retryable_job_errors_become_airflow_failures() -> None:
    def fail() -> None:
        raise JobConfigurationError("bad config")

    with pytest.raises(AirflowFailException, match="bad config") as exc_info:
        run_with_airflow_error_policy(fail)
    assert isinstance(exc_info.value.__cause__, JobConfigurationError)


def test_retryable_job_errors_pass_through_unchanged() -> None:
    error = RetryableJobError("temporary")

    def fail() -> None:
        raise error

    with pytest.raises(RetryableJobError) as exc_info:
        run_with_airflow_error_policy(fail)
    assert exc_info.value is error
