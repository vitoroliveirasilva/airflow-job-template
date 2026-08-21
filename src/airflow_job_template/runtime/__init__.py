"""
Public runtime surface for DAG authors

Pure dataclasses and errors stay importable without Airflow. The single-task factory loads lazily only when a DAG requests it.
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
