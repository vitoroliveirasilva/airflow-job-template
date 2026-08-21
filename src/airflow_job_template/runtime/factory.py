"""Minimal factory for one-task Python jobs"""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from time import perf_counter
from typing import Any

from airflow.sdk import DAG, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException

from airflow_job_template.observability.logging import log_event

from .context import JobResult, JobRunContext, job_run_context_from_airflow
from .errors import NonRetryableJobError
from .spec import JobSpec

JobCallable = Callable[[JobRunContext], JobResult | None]


def build_single_task_dag(
    *,
    spec: JobSpec,
    job_callable: JobCallable,
    task_id: str = "execute",
    task_overrides: Mapping[str, Any] | None = None,
    dag_overrides: Mapping[str, Any] | None = None,
) -> DAG:
    """Build a DAG containing one TaskFlow task and no integration-specific behavior"""

    if not isinstance(spec, JobSpec):
        raise TypeError("spec must be a JobSpec")
    if not callable(job_callable):
        raise TypeError("job_callable must be callable")
    if not isinstance(task_id, str) or not task_id.strip():
        raise ValueError("task_id must be a non-blank string")
    if task_overrides is not None and not isinstance(task_overrides, Mapping):
        raise TypeError("task_overrides must be a mapping or None")
    if dag_overrides is not None and not isinstance(dag_overrides, Mapping):
        raise TypeError("dag_overrides must be a mapping or None")
    if task_overrides and "task_id" in task_overrides:
        raise ValueError("pass task_id via the dedicated task_id argument")

    dag_kwargs = spec.as_dag_kwargs(**dict(dag_overrides or {}))
    schedule = dag_kwargs.pop("schedule")
    task_kwargs = spec.task_policy.as_task_kwargs()
    task_kwargs.update(dict(task_overrides or {}))

    with DAG(schedule=schedule, **dag_kwargs) as dag:

        @task(task_id=task_id, **task_kwargs)
        def execute_job() -> dict[str, int | str] | None:
            context = job_run_context_from_airflow(get_current_context())
            logger = logging.getLogger("airflow.task")
            started = perf_counter()
            log_event(logger, "job_started", context=context)
            try:
                result = job_callable(context)
            except NonRetryableJobError as exc:
                log_event(
                    logger,
                    "job_failed_non_retryable",
                    context=context,
                    error_type=type(exc).__name__,
                )
                raise AirflowFailException(str(exc)) from exc

            if result is not None and not isinstance(result, JobResult):
                raise AirflowFailException(
                    "Simple jobs must return JobResult or None; store large payloads externally"
                )

            payload = result.to_xcom() if result is not None else None
            log_event(
                logger,
                "job_completed",
                context=context,
                duration_seconds=round(perf_counter() - started, 3),
                **(payload or {}),
            )
            return payload

        execute_job()

    return dag
