from datetime import timedelta

import pytest

from airflow_job_template.runtime import JobConfigurationError, TaskPolicy


def test_task_policy_defaults_are_conservative() -> None:
    policy = TaskPolicy()
    kwargs = policy.as_task_kwargs()

    assert kwargs["retries"] == 2
    assert kwargs["retry_delay"] == timedelta(minutes=5)
    assert kwargs["execution_timeout"] == timedelta(minutes=30)
    assert kwargs["retry_exponential_backoff"] is True
    assert "pool" not in kwargs
    assert "queue" not in kwargs


def test_task_policy_keeps_optional_pool_and_queue_explicit() -> None:
    policy = TaskPolicy(pool="rpa_pool", queue="rpa")
    assert policy.as_task_kwargs()["pool"] == "rpa_pool"
    assert policy.as_task_kwargs()["queue"] == "rpa"


@pytest.mark.parametrize(
    "kwargs",
    [
        {"retries": -1},
        {"retry_delay": timedelta(seconds=-1)},
        {"execution_timeout": timedelta(0)},
        {"priority_weight": 0},
        {"pool": " "},
    ],
)
def test_task_policy_rejects_invalid_values(kwargs: dict) -> None:
    with pytest.raises(JobConfigurationError):
        TaskPolicy(**kwargs)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"retries": True},
        {"retry_delay": 5},
        {"execution_timeout": 30},
        {"retry_exponential_backoff": 1},
        {"max_retry_delay": 30},
        {"priority_weight": True},
        {"queue": 123},
    ],
)
def test_task_policy_rejects_wrong_runtime_types(kwargs: dict) -> None:
    with pytest.raises(JobConfigurationError):
        TaskPolicy(**kwargs)
