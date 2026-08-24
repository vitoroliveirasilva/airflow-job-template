"""Public runtime surface for DAG authors.

Pure dataclasses and errors stay importable without Airflow. Airflow-specific adapters load
lazily only when a DAG requests them.
"""

from .context import JobResult, JobRunContext, job_run_context_from_airflow
from .errors import (
    JobConfigurationError,
    JobError,
    NonRetryableJobError,
    RetryableJobError,
)
from .policies import TaskPolicy
from .spec import DEFAULT_START_DATE, JobSpec

__all__ = [
    "DEFAULT_START_DATE",
    "JobConfigurationError",
    "JobError",
    "JobResult",
    "JobRunContext",
    "JobSpec",
    "NonRetryableJobError",
    "RetryableJobError",
    "TaskPolicy",
    "build_single_task_dag",
    "job_run_context_from_airflow",
    "run_with_airflow_error_policy",
]


def __getattr__(name: str):
    if name == "build_single_task_dag":
        from .factory import build_single_task_dag

        return build_single_task_dag
    if name == "run_with_airflow_error_policy":
        from .airflow import run_with_airflow_error_policy

        return run_with_airflow_error_policy
    raise AttributeError(name)
