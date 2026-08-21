"""
Public runtime surface for DAG authors

The pure dataclasses/errors stay importable without importing Airflow itself. The single-task factory is loaded lazily when a DAG actually asks for it.
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
]


def __getattr__(name: str):
    if name == "build_single_task_dag":
        from .factory import build_single_task_dag

        return build_single_task_dag
    raise AttributeError(name)
