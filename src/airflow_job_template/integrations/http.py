"""Runtime-only HTTP helper backed by an Airflow Connection/Hook"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from typing import Any, Protocol

from airflow_job_template.runtime.errors import (
    JobConfigurationError,
    NonRetryableJobError,
    RetryableJobError,
)


class _Response(Protocol):
    status_code: int

    def json(self) -> Any: ...


class _HttpHook(Protocol):
    def run(self, endpoint: str, **kwargs: Any) -> _Response: ...


HookFactory = Callable[[str, str], _HttpHook]


@dataclass(frozen=True, slots=True)
class HttpTimeout:
    """Explicit connect/read timeout pair passed to ``requests`` via HttpHook"""

    connect_seconds: float = 5.0
    read_seconds: float = 30.0

    def __post_init__(self) -> None:
        if self.connect_seconds <= 0 or self.read_seconds <= 0:
            raise JobConfigurationError("HTTP timeouts must be > 0")

    def as_requests_timeout(self) -> tuple[float, float]:
        return (self.connect_seconds, self.read_seconds)


def _default_hook_factory(conn_id: str, method: str) -> _HttpHook:
    try:
        from airflow.providers.http.hooks.http import HttpHook
    except ImportError as exc:
        raise JobConfigurationError(
            "HTTP provider is not installed; install requirements/optional/http.txt"
        ) from exc
    return HttpHook(method=method, http_conn_id=conn_id)


class HttpClient:
    """Thin HTTP client with zero internal retries; Airflow owns task-level retries"""

    def __init__(
        self,
        conn_id: str,
        *,
        timeout: HttpTimeout | None = None,
        hook_factory: HookFactory | None = None,
    ) -> None:
        if not conn_id.strip():
            raise JobConfigurationError("conn_id cannot be blank")
        self.conn_id = conn_id
        self.timeout = timeout or HttpTimeout()
        self._hook_factory = hook_factory or _default_hook_factory

    def request_json(
        self,
        method: str,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        data: Any = None,
        json_body: Any = None,
        headers: Mapping[str, str] | None = None,
        retry_safe: bool | None = None,
    ) -> Any:
        """
        Perform one request, classify status codes, and decode JSON safely

        Mutating methods are not considered retry-safe by default. A caller may set ``retry_safe=True`` only after making the operation idempotent (for example with a deterministic idempotency key or a verified UPSERT-like remote contract).
        """

        normalized_method = method.upper().strip()
        if normalized_method not in {
            "GET",
            "HEAD",
            "POST",
            "PUT",
            "PATCH",
            "DELETE",
            "OPTIONS",
        }:
            raise JobConfigurationError(f"unsupported HTTP method {method!r}")
        if not endpoint or not endpoint.strip():
            raise JobConfigurationError("endpoint cannot be blank")

        can_retry = (
            retry_safe
            if retry_safe is not None
            else normalized_method
            in {
                "GET",
                "HEAD",
                "OPTIONS",
            }
        )
        hook = self._hook_factory(self.conn_id, normalized_method)
        try:
            response = hook.run(
                endpoint=endpoint,
                data=data,
                headers=dict(headers or {}),
                extra_options={
                    "timeout": self.timeout.as_requests_timeout(),
                    "check_response": False,
                },
                params=dict(params or {}),
                json=json_body,
            )
        except RetryableJobError:
            raise
        except Exception as exc:
            error_type = RetryableJobError if can_retry else NonRetryableJobError
            raise error_type(
                f"HTTP request failed without a response for conn_id={self.conn_id!r}; "
                f"retry_safe={can_retry}"
            ) from exc

        status = int(response.status_code)
        if status in {408, 425, 429} or 500 <= status <= 599:
            error_type = RetryableJobError if can_retry else NonRetryableJobError
            raise error_type(
                f"transient HTTP status {status} for conn_id={self.conn_id!r}; "
                f"retry_safe={can_retry}"
            )
        if status >= 400:
            raise NonRetryableJobError(
                f"non-retryable HTTP status {status} for conn_id={self.conn_id!r}"
            )
        try:
            return response.json()
        except (TypeError, ValueError) as exc:
            raise NonRetryableJobError(
                f"response from conn_id={self.conn_id!r} is not valid JSON"
            ) from exc

    def iter_offset_items(
        self,
        endpoint: str,
        *,
        item_key: str,
        page_size: int = 100,
        start_page: int = 1,
        page_param: str = "page",
        page_size_param: str = "page_size",
        max_pages: int = 10_000,
        params: Mapping[str, Any] | None = None,
    ) -> Iterator[Any]:
        """Iterate a common page/page-size API without pretending all APIs paginate alike"""

        if page_size < 1 or start_page < 1 or max_pages < 1:
            raise JobConfigurationError(
                "page_size, start_page and max_pages must be >= 1"
            )
        if not item_key.strip():
            raise JobConfigurationError("item_key cannot be blank")

        base_params = dict(params or {})
        page = start_page
        for _ in range(max_pages):
            request_params = {
                **base_params,
                page_param: page,
                page_size_param: page_size,
            }
            payload = self.request_json("GET", endpoint, params=request_params)
            if not isinstance(payload, Mapping):
                raise NonRetryableJobError(
                    "paginated HTTP response must be a JSON object"
                )
            items = payload.get(item_key)
            if not isinstance(items, list):
                raise NonRetryableJobError(
                    f"response field {item_key!r} must be a JSON array"
                )
            yield from items
            if len(items) < page_size:
                return
            page += 1
        raise NonRetryableJobError(
            "pagination exceeded max_pages; check the API contract"
        )
