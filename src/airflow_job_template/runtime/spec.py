"""Typed, intentionally small DAG specification"""

import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Mapping

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
        if not _DAG_ID_RE.fullmatch(self.dag_id) or len(self.dag_id) > 100:
            raise JobConfigurationError(
                "dag_id must be snake_case, start with a letter, and be <= 100 characters"
            )
        if not self.description.strip():
            raise JobConfigurationError("description cannot be blank")
        if self.schedule is not None and not self.schedule.strip():
            raise JobConfigurationError(
                "schedule must be None or a non-blank Airflow schedule"
            )
        if self.start_date.tzinfo is None or self.start_date.utcoffset() is None:
            raise JobConfigurationError(
                "start_date must be timezone-aware and deterministic"
            )
        if not self.owner.strip():
            raise JobConfigurationError("owner cannot be blank")
        if self.max_active_runs < 1:
            raise JobConfigurationError("max_active_runs must be >= 1")
        if len(self.tags) > 10:
            raise JobConfigurationError("use at most 10 focused DAG tags")
        for tag in self.tags:
            if not _TAG_RE.fullmatch(tag) or len(tag) > 50:
                raise JobConfigurationError(
                    f"invalid tag {tag!r}; use short alphanumeric tags with . _ : or -"
                )
        for name in self.params:
            if not isinstance(name, str) or not name.strip():
                raise JobConfigurationError("Param names must be non-blank strings")

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
