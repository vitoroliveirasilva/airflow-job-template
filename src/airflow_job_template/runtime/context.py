"""Runtime context passed to normal Python job logic"""

from collections.abc import Mapping
from dataclasses import dataclass, fields
from datetime import datetime
from types import MappingProxyType
from typing import Any

from airflow_job_template._security import contains_sensitive_uri_data

from .errors import JobConfigurationError

_MAX_ARTIFACT_URI_LENGTH = 4096
_MAX_BATCH_ID_LENGTH = 256


def _validate_optional_datetime(name: str, value: Any) -> None:
    if value is None:
        return
    if not isinstance(value, datetime):
        raise JobConfigurationError(f"{name} must be a datetime or None")
    if value.tzinfo is None or value.utcoffset() is None:
        raise JobConfigurationError(f"{name} must be timezone-aware when set")


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
        for name in ("dag_id", "task_id", "run_id"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise JobConfigurationError(f"{name} must be a non-blank string at runtime")
            if value != value.strip():
                raise JobConfigurationError(f"{name} must not contain surrounding whitespace")
        if isinstance(self.try_number, bool) or not isinstance(self.try_number, int):
            raise JobConfigurationError("try_number must be an integer >= 1")
        if self.try_number < 1:
            raise JobConfigurationError("try_number must be >= 1")
        for name in ("logical_date", "data_interval_start", "data_interval_end"):
            _validate_optional_datetime(name, getattr(self, name))
        if (
            self.data_interval_start is not None
            and self.data_interval_end is not None
            and self.data_interval_start > self.data_interval_end
        ):
            raise JobConfigurationError("data_interval_start must not be after data_interval_end")
        if not isinstance(self.params, Mapping):
            raise JobConfigurationError("params must be a mapping at runtime")
        normalized_params = dict(self.params)
        for name in normalized_params:
            if not isinstance(name, str) or not name.strip():
                raise JobConfigurationError("runtime Param names must be non-blank strings")
            if name != name.strip():
                raise JobConfigurationError(
                    "runtime Param names must not contain surrounding whitespace"
                )
        object.__setattr__(self, "params", MappingProxyType(normalized_params))


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
            if value is not None and (
                isinstance(value, bool) or not isinstance(value, int) or value < 0
            ):
                raise JobConfigurationError(f"JobResult.{name} must be an integer >= 0 when set")
        for name in ("artifact_uri", "batch_id"):
            value = getattr(self, name)
            if value is not None and (not isinstance(value, str) or not value.strip()):
                raise JobConfigurationError(f"JobResult.{name} must be a non-blank string when set")
        if self.artifact_uri is not None:
            if len(self.artifact_uri) > _MAX_ARTIFACT_URI_LENGTH:
                raise JobConfigurationError(
                    f"JobResult.artifact_uri must be <= {_MAX_ARTIFACT_URI_LENGTH} characters"
                )
            if contains_sensitive_uri_data(self.artifact_uri):
                raise JobConfigurationError(
                    "JobResult.artifact_uri must not contain credentials or tokens"
                )
        if self.batch_id is not None and len(self.batch_id) > _MAX_BATCH_ID_LENGTH:
            raise JobConfigurationError(
                f"JobResult.batch_id must be <= {_MAX_BATCH_ID_LENGTH} characters"
            )

    def to_xcom(self) -> dict[str, int | str]:
        """Return only explicitly populated scalar metadata."""

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

    if not isinstance(context, Mapping):
        raise JobConfigurationError("Airflow context must be a mapping")

    ti = context.get("ti") or context.get("task_instance")
    dag = context.get("dag")
    task = context.get("task")
    dag_run = context.get("dag_run")

    dag_id = _value(dag, "dag_id") or _value(ti, "dag_id") or context.get("dag_id")
    task_id = _value(task, "task_id") or _value(ti, "task_id") or context.get("task_id")
    run_id = _value(dag_run, "run_id") or _value(ti, "run_id") or context.get("run_id")
    try_number = _value(ti, "try_number")
    if try_number is None:
        try_number = context.get("try_number")
    if try_number is None:
        try_number = 1

    if not all(isinstance(value, str) and value.strip() for value in (dag_id, task_id, run_id)):
        raise JobConfigurationError("Airflow context is missing dag_id, task_id or run_id")
    if isinstance(try_number, bool) or not isinstance(try_number, int) or try_number < 1:
        raise JobConfigurationError("Airflow context try_number must be an integer >= 1")

    params = context.get("params")
    if params is None:
        params = {}
    if not isinstance(params, Mapping):
        raise JobConfigurationError("Airflow context params must be a mapping")

    return JobRunContext(
        dag_id=dag_id,
        task_id=task_id,
        run_id=run_id,
        try_number=try_number,
        logical_date=context.get("logical_date") or _value(dag_run, "logical_date"),
        data_interval_start=context.get("data_interval_start")
        or _value(dag_run, "data_interval_start"),
        data_interval_end=context.get("data_interval_end") or _value(dag_run, "data_interval_end"),
        params=params,
    )
