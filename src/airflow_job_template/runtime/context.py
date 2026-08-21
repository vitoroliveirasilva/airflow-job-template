"""Runtime context passed to normal Python job logic"""

from collections.abc import Mapping
from dataclasses import dataclass, fields
from datetime import datetime
from types import MappingProxyType
from typing import Any

from .errors import JobConfigurationError


@dataclass(frozen=True, slots=True)
class JobRunContext:
    """Small Airflow-independent execution context for job code and unit tests"""

    dag_id: str
    task_id: str
    run_id: str
    try_number: int
    logical_date: datetime | None
    data_interval_start: datetime | None
    data_interval_end: datetime | None
    params: Mapping[str, Any]

    def __post_init__(self) -> None:
        object.__setattr__(self, "params", MappingProxyType(dict(self.params)))
        if not self.dag_id or not self.task_id or not self.run_id:
            raise JobConfigurationError("dag_id, task_id and run_id are required at runtime")
        if self.try_number < 1:
            raise JobConfigurationError("try_number must be >= 1")


@dataclass(frozen=True, slots=True)
class JobResult:
    """Small execution metadata safe to return through XCom"""

    processed: int | None = None
    created: int | None = None
    updated: int | None = None
    skipped: int | None = None
    artifact_uri: str | None = None
    batch_id: str | None = None

    def __post_init__(self) -> None:
        for name in ("processed", "created", "updated", "skipped"):
            value = getattr(self, name)
            if value is not None and value < 0:
                raise JobConfigurationError(f"JobResult.{name} must be >= 0")
        for name in ("artifact_uri", "batch_id"):
            value = getattr(self, name)
            if value is not None and not value.strip():
                raise JobConfigurationError(f"JobResult.{name} cannot be blank")

    def to_xcom(self) -> dict[str, int | str]:
        """Return only explicitly populated scalar metadata"""

        result: dict[str, int | str] = {}
        for field_info in fields(self):
            value = getattr(self, field_info.name)
            if value is not None:
                result[field_info.name] = value
        return result


def _value(obj: Any, name: str) -> Any:
    if obj is None:
        return None
    if isinstance(obj, Mapping):
        return obj.get(name)
    return getattr(obj, name, None)


def job_run_context_from_airflow(context: Mapping[str, Any]) -> JobRunContext:
    """Adapt an Airflow task context mapping without importing Airflow internals"""

    ti = context.get("ti") or context.get("task_instance")
    dag = context.get("dag")
    task = context.get("task")
    dag_run = context.get("dag_run")

    dag_id = _value(dag, "dag_id") or _value(ti, "dag_id") or context.get("dag_id")
    task_id = _value(task, "task_id") or _value(ti, "task_id") or context.get("task_id")
    run_id = _value(dag_run, "run_id") or _value(ti, "run_id") or context.get("run_id")
    try_number = _value(ti, "try_number") or context.get("try_number") or 1

    if not all(isinstance(value, str) and value for value in (dag_id, task_id, run_id)):
        raise JobConfigurationError("Airflow context is missing dag_id, task_id or run_id")

    params = context.get("params") or {}
    if not isinstance(params, Mapping):
        raise JobConfigurationError("Airflow context params must be a mapping")

    return JobRunContext(
        dag_id=dag_id,
        task_id=task_id,
        run_id=run_id,
        try_number=int(try_number),
        logical_date=context.get("logical_date") or _value(dag_run, "logical_date"),
        data_interval_start=context.get("data_interval_start"),
        data_interval_end=context.get("data_interval_end"),
        params=params,
    )
