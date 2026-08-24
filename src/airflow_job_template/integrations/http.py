"""Runtime-only HTTP helper backed by an Airflow Connection/Hook"""

from __future__ import annotations

import math
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from numbers import Real
from typing import Any, Protocol
from urllib.parse import urlsplit

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


def _positive_finite_seconds(value: float, *, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, Real):
        raise JobConfigurationError(f"HTTP {name} timeout must be a finite number > 0")
    normalized = float(value)
    if not math.isfinite(normalized) or normalized <= 0:
        raise JobConfigurationError(f"HTTP {name} timeout must be a finite number > 0")
    return normalized


def _exception_class_names(exc: BaseException) -> set[str]:
    """Collect class names through normal exception chaining without inspecting messages"""

    names: set[str] = set()
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        names.update(cls.__name__ for cls in type(current).__mro__)
        current = current.__cause__ or current.__context__
    return names


def _validated_headers(headers: Mapping[str, str] | None) -> dict[str, str]:
    if headers is None:
        return {}
    if not isinstance(headers, Mapping):
        raise JobConfigurationError("headers must be a mapping when set")

    result: dict[str, str] = {}
    for name, value in headers.items():
        if not isinstance(name, str) or not name.strip():
            raise JobConfigurationError("HTTP header names must be non-blank strings")
        if name != name.strip() or "\r" in name or "\n" in name:
            raise JobConfigurationError("HTTP header names must not contain whitespace controls")
        if not isinstance(value, str):
            raise JobConfigurationError(f"HTTP header {name!r} value must be a string")
        if "\r" in value or "\n" in value:
            raise JobConfigurationError(f"HTTP header {name!r} value must not contain CR/LF")
        result[name] = value
    return result


@dataclass(frozen=True, slots=True)
class HttpTimeout:
    """Explicit connect/read timeout pair passed to ``requests`` via HttpHook"""

    connect_seconds: float = 5.0
    read_seconds: float = 30.0

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "connect_seconds",
            _positive_finite_seconds(self.connect_seconds, name="connect"),
        )
        object.__setattr__(
            self,
            "read_seconds",
            _positive_finite_seconds(self.read_seconds, name="read"),
        )

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
        if not isinstance(conn_id, str) or not conn_id.strip():
            raise JobConfigurationError("conn_id must be a non-blank string")
        if conn_id != conn_id.strip():
            raise JobConfigurationError("conn_id must not contain surrounding whitespace")
        if timeout is not None and not isinstance(timeout, HttpTimeout):
            raise JobConfigurationError("timeout must be an HttpTimeout")
        if hook_factory is not None and not callable(hook_factory):
            raise JobConfigurationError("hook_factory must be callable")
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
        """Perform one request, classify status codes, and decode JSON safely.

        Mutating methods are not considered retry-safe by default. A caller may set
        ``retry_safe=True`` only after making the operation idempotent, for example with a
        deterministic idempotency key or a verified UPSERT-like remote contract.
        """

        if not isinstance(method, str):
            raise JobConfigurationError("HTTP method must be a string")
        normalized_method = method.upper().strip()
        if normalized_method not in {"GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"}:
            raise JobConfigurationError(f"unsupported HTTP method {method!r}")
        if not isinstance(endpoint, str) or not endpoint.strip():
            raise JobConfigurationError("endpoint must be a non-blank string")
        if endpoint != endpoint.strip():
            raise JobConfigurationError("endpoint must not contain surrounding whitespace")
        if "\r" in endpoint or "\n" in endpoint:
            raise JobConfigurationError("endpoint must not contain CR/LF characters")
        parsed_endpoint = urlsplit(endpoint)
        if parsed_endpoint.scheme or parsed_endpoint.netloc:
            raise JobConfigurationError(
                "endpoint must be relative; configure the host in the Airflow Connection"
            )
        if parsed_endpoint.fragment:
            raise JobConfigurationError("endpoint must not contain a URI fragment")
        if data is not None and json_body is not None:
            raise JobConfigurationError("set either data or json_body, not both")
        if params is not None and not isinstance(params, Mapping):
            raise JobConfigurationError("params must be a mapping when set")
        request_headers = _validated_headers(headers)
        if retry_safe is not None and not isinstance(retry_safe, bool):
            raise JobConfigurationError("retry_safe must be a boolean or None")

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
                headers=request_headers,
                extra_options={
                    "timeout": self.timeout.as_requests_timeout(),
                    "check_response": False,
                },
                params=dict(params or {}),
                json=json_body,
            )
        except (JobConfigurationError, NonRetryableJobError, RetryableJobError):
            raise
        except Exception as exc:
            class_names = _exception_class_names(exc)
            if "AirflowNotFoundException" in class_names:
                raise JobConfigurationError(
                    f"Airflow Connection {self.conn_id!r} was not found"
                ) from exc
            permanently_invalid = bool(
                class_names & {"SSLError", "InvalidURL", "MissingSchema", "InvalidSchema"}
            )
            error_type = (
                NonRetryableJobError if permanently_invalid or not can_retry else RetryableJobError
            )
            raise error_type(
                f"HTTP request failed without a response for conn_id={self.conn_id!r}; "
                f"retry_safe={can_retry}"
            ) from exc

        try:
            status = int(response.status_code)
        except (AttributeError, TypeError, ValueError) as exc:
            raise NonRetryableJobError(
                f"HTTP hook returned an invalid status for conn_id={self.conn_id!r}"
            ) from exc
        if not 100 <= status <= 599:
            raise NonRetryableJobError(
                f"HTTP hook returned out-of-range status {status} for conn_id={self.conn_id!r}"
            )
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
        if status < 200 or 300 <= status <= 399:
            raise NonRetryableJobError(
                f"unexpected HTTP status {status} for conn_id={self.conn_id!r}"
            )
        if normalized_method == "HEAD" or status in {204, 205}:
            return None
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

        for name, value in (
            ("page_size", page_size),
            ("start_page", start_page),
            ("max_pages", max_pages),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise JobConfigurationError(f"{name} must be an integer >= 1")
        for name, value in (
            ("item_key", item_key),
            ("page_param", page_param),
            ("page_size_param", page_size_param),
        ):
            if not isinstance(value, str) or not value.strip():
                raise JobConfigurationError(f"{name} must be a non-blank string")
            if value != value.strip():
                raise JobConfigurationError(f"{name} must not contain surrounding whitespace")
        if page_param == page_size_param:
            raise JobConfigurationError("page_param and page_size_param must be different")
        if params is not None and not isinstance(params, Mapping):
            raise JobConfigurationError("params must be a mapping when set")

        base_params = dict(params or {})
        conflicts = {page_param, page_size_param} & base_params.keys()
        if conflicts:
            names = ", ".join(sorted(conflicts))
            raise JobConfigurationError(
                f"pagination params must not redefine managed fields: {names}"
            )

        page = start_page
        for _ in range(max_pages):
            request_params = {
                **base_params,
                page_param: page,
                page_size_param: page_size,
            }
            payload = self.request_json("GET", endpoint, params=request_params)
            if not isinstance(payload, Mapping):
                raise NonRetryableJobError("paginated HTTP response must be a JSON object")
            items = payload.get(item_key)
            if not isinstance(items, list):
                raise NonRetryableJobError(f"response field {item_key!r} must be a JSON array")
            yield from items
            if len(items) < page_size:
                return
            page += 1
        raise NonRetryableJobError("pagination exceeded max_pages; check the API contract")
