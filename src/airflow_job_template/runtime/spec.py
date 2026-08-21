"""Typed, intentionally small DAG specification"""

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from types import MappingProxyType
from typing import Any

from .errors import JobConfigurationError
from .policies import TaskPolicy

_DAG_ID_RE = re.compile(r"^[a-z][a-z0-9_]*$")
_TAG_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")
DEFAULT_START_DATE = datetime(2024, 1, 1, tzinfo=UTC)


@dataclass(frozen=True, slots=True)
class JobSpec:
    """Repeated DAG configuration for the simple path and explicit workflows"""

    dag_id: str
    description: str
    schedule: str | None = None
    start_date: datetime = DEFAULT_START_DATE
    tags: tuple[str, ...] = ()
    owner: str = "airflow"
    catchup: bool = False
    max_active_runs: int = 1
    params: Mapping[str, Any] = field(default_factory=dict)
    task_policy: TaskPolicy = field(default_factory=TaskPolicy)
    doc_md: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.dag_id, str) or not _DAG_ID_RE.fullmatch(self.dag_id):
            raise JobConfigurationError("dag_id must be snake_case and start with a letter")
        if len(self.dag_id) > 100:
            raise JobConfigurationError("dag_id must be <= 100 characters")
        if not isinstance(self.description, str) or not self.description.strip():
            raise JobConfigurationError("description must be a non-blank string")
        if self.schedule is not None and (
            not isinstance(self.schedule, str) or not self.schedule.strip()
        ):
            raise JobConfigurationError("schedule must be None or a non-blank Airflow schedule")
        if not isinstance(self.start_date, datetime):
            raise JobConfigurationError("start_date must be a datetime")
        if self.start_date.tzinfo is None or self.start_date.utcoffset() is None:
            raise JobConfigurationError("start_date must be timezone-aware and deterministic")
        if not isinstance(self.owner, str) or not self.owner.strip():
            raise JobConfigurationError("owner must be a non-blank string")
        if not isinstance(self.catchup, bool):
            raise JobConfigurationError("catchup must be a boolean")
        if isinstance(self.max_active_runs, bool) or not isinstance(self.max_active_runs, int):
            raise JobConfigurationError("max_active_runs must be an integer >= 1")
        if self.max_active_runs < 1:
            raise JobConfigurationError("max_active_runs must be >= 1")
        if not isinstance(self.task_policy, TaskPolicy):
            raise JobConfigurationError("task_policy must be a TaskPolicy")
        if self.doc_md is not None and not isinstance(self.doc_md, str):
            raise JobConfigurationError("doc_md must be a string or None")

        if isinstance(self.tags, str):
            raise JobConfigurationError("tags must be an iterable of tag strings")
        try:
            normalized_tags = tuple(self.tags)
        except TypeError as exc:
            raise JobConfigurationError("tags must be an iterable of tag strings") from exc
        if len(normalized_tags) > 10:
            raise JobConfigurationError("use at most 10 focused DAG tags")
        for tag in normalized_tags:
            if not isinstance(tag, str) or not _TAG_RE.fullmatch(tag) or len(tag) > 50:
                raise JobConfigurationError(
                    f"invalid tag {tag!r}; use short alphanumeric tags with . _ : or -"
                )

        if not isinstance(self.params, Mapping):
            raise JobConfigurationError("params must be a mapping")
        normalized_params = dict(self.params)
        for name in normalized_params:
            if not isinstance(name, str) or not name.strip():
                raise JobConfigurationError("Param names must be non-blank strings")

        object.__setattr__(self, "tags", normalized_tags)
        object.__setattr__(self, "params", MappingProxyType(normalized_params))

    def as_dag_kwargs(self, **overrides: Any) -> dict[str, Any]:
        """Translate the spec to public ``DAG``/``@dag`` keyword arguments"""

        kwargs: dict[str, Any] = {
            "dag_id": self.dag_id,
            "description": self.description,
            "schedule": self.schedule,
            "start_date": self.start_date,
            "catchup": self.catchup,
            "max_active_runs": self.max_active_runs,
            "tags": list(self.tags),
            "default_args": {"owner": self.owner},
        }
        if self.params:
            kwargs["params"] = dict(self.params)
        if self.doc_md:
            kwargs["doc_md"] = self.doc_md
        kwargs.update(overrides)
        return kwargs
