"""Operational defaults that remain visible and easy to override"""

from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from .errors import JobConfigurationError


@dataclass(frozen=True, slots=True)
class TaskPolicy:
    """Conservative defaults for a normal external-integration task"""

    retries: int = 2
    retry_delay: timedelta = timedelta(minutes=5)
    execution_timeout: timedelta = timedelta(minutes=30)
    retry_exponential_backoff: bool = True
    max_retry_delay: timedelta | None = timedelta(minutes=30)
    pool: str | None = None
    queue: str | None = None
    priority_weight: int = 1

    def __post_init__(self) -> None:
        if self.retries < 0:
            raise JobConfigurationError("TaskPolicy.retries must be >= 0")
        if self.retry_delay < timedelta(0):
            raise JobConfigurationError("TaskPolicy.retry_delay must be >= 0")
        if self.execution_timeout <= timedelta(0):
            raise JobConfigurationError("TaskPolicy.execution_timeout must be > 0")
        if self.max_retry_delay is not None and self.max_retry_delay <= timedelta(0):
            raise JobConfigurationError(
                "TaskPolicy.max_retry_delay must be > 0 when set"
            )
        if self.priority_weight < 1:
            raise JobConfigurationError("TaskPolicy.priority_weight must be >= 1")
        for field_name, value in (("pool", self.pool), ("queue", self.queue)):
            if value is not None and not value.strip():
                raise JobConfigurationError(f"TaskPolicy.{field_name} cannot be blank")

    def as_task_kwargs(self) -> dict[str, Any]:
        """Return TaskFlow/operator kwargs without meaningless ``None`` values"""

        kwargs: dict[str, Any] = {
            "retries": self.retries,
            "retry_delay": self.retry_delay,
            "execution_timeout": self.execution_timeout,
            "retry_exponential_backoff": self.retry_exponential_backoff,
            "priority_weight": self.priority_weight,
        }
        if self.max_retry_delay is not None:
            kwargs["max_retry_delay"] = self.max_retry_delay
        if self.pool is not None:
            kwargs["pool"] = self.pool
        if self.queue is not None:
            kwargs["queue"] = self.queue
        return kwargs
