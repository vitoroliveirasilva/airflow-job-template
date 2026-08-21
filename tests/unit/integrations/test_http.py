import pytest

from airflow_job_template.integrations.http import HttpClient, HttpTimeout
from airflow_job_template.runtime import (
    JobConfigurationError,
    NonRetryableJobError,
    RetryableJobError,
)


class Response:
    def __init__(self, status_code: int, payload):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


class Hook:
    def __init__(self, responses):
        self.responses = iter(responses)
        self.calls = []

    def run(self, endpoint: str, **kwargs):
        self.calls.append((endpoint, kwargs))
        return next(self.responses)


def test_request_uses_explicit_timeout_and_decodes_json() -> None:
    hook = Hook([Response(200, {"ok": True})])
    client = HttpClient(
        "crm_api",
        timeout=HttpTimeout(2, 8),
        hook_factory=lambda _conn_id, _method: hook,
    )

    assert client.request_json("GET", "/health") == {"ok": True}
    assert hook.calls[0][1]["extra_options"]["timeout"] == (2, 8)


@pytest.mark.parametrize("status", [408, 429, 500, 502, 503])
def test_transient_status_is_retryable(status: int) -> None:
    client = HttpClient(
        "crm_api",
        hook_factory=lambda _conn_id, _method: Hook([Response(status, {})]),
    )
    with pytest.raises(RetryableJobError):
        client.request_json("GET", "/customers")


def test_functional_400_is_non_retryable() -> None:
    client = HttpClient(
        "crm_api",
        hook_factory=lambda _conn_id, _method: Hook([Response(400, {})]),
    )
    with pytest.raises(NonRetryableJobError):
        client.request_json("GET", "/customers")


def test_offset_pagination_stops_on_short_page() -> None:
    hook = Hook(
        [
            Response(200, {"items": [{"id": 1}, {"id": 2}]}),
            Response(200, {"items": [{"id": 3}]}),
        ]
    )
    client = HttpClient("crm_api", hook_factory=lambda _conn_id, _method: hook)
    items = list(client.iter_offset_items("/customers", item_key="items", page_size=2))
    assert [item["id"] for item in items] == [1, 2, 3]


def test_mutating_request_is_not_retryable_without_explicit_idempotency() -> None:
    client = HttpClient(
        "crm_api",
        hook_factory=lambda _conn_id, _method: Hook([Response(503, {})]),
    )
    with pytest.raises(NonRetryableJobError, match="retry_safe=False"):
        client.request_json("POST", "/customers", json_body={"name": "Ada"})


def test_mutating_request_can_opt_into_retry_after_idempotency_is_established() -> None:
    client = HttpClient(
        "crm_api",
        hook_factory=lambda _conn_id, _method: Hook([Response(503, {})]),
    )
    with pytest.raises(RetryableJobError, match="retry_safe=True"):
        client.request_json(
            "POST",
            "/customers",
            json_body={"name": "Ada"},
            headers={"Idempotency-Key": "run-123"},
            retry_safe=True,
        )


def test_missing_airflow_connection_fails_fast_without_retry() -> None:
    class AirflowNotFoundException(RuntimeError):
        pass

    class MissingConnectionHook:
        def run(self, endpoint: str, **kwargs):
            raise AirflowNotFoundException("missing")

    client = HttpClient(
        "missing_api",
        hook_factory=lambda _conn_id, _method: MissingConnectionHook(),
    )
    with pytest.raises(JobConfigurationError, match="missing_api"):
        client.request_json("GET", "/customers")


def test_tls_verification_failure_is_not_retried() -> None:
    class SSLError(RuntimeError):
        pass

    class TlsFailureHook:
        def run(self, endpoint: str, **kwargs):
            raise SSLError("certificate verify failed")

    client = HttpClient(
        "crm_api",
        hook_factory=lambda _conn_id, _method: TlsFailureHook(),
    )
    with pytest.raises(NonRetryableJobError):
        client.request_json("GET", "/customers")


def test_timeout_rejects_nan_and_non_numeric_values() -> None:
    with pytest.raises(JobConfigurationError, match="finite number"):
        HttpTimeout(float("nan"), 1)
    with pytest.raises(JobConfigurationError, match="finite number"):
        HttpTimeout("5", 1)


@pytest.mark.parametrize(("method", "status"), [("HEAD", 200), ("GET", 204), ("POST", 205)])
def test_success_without_response_body_returns_none(method: str, status: int) -> None:
    class EmptyResponse:
        status_code = status

        def json(self):
            raise AssertionError("json() must not be called when the response has no body")

    client = HttpClient(
        "crm_api",
        hook_factory=lambda _conn_id, _method: Hook([EmptyResponse()]),
    )
    assert client.request_json(method, "/resource", retry_safe=False) is None


def test_invalid_hook_status_is_non_retryable() -> None:
    client = HttpClient(
        "crm_api",
        hook_factory=lambda _conn_id, _method: Hook([Response("not-a-status", {})]),
    )
    with pytest.raises(NonRetryableJobError, match="invalid status"):
        client.request_json("GET", "/customers")


def test_pagination_rejects_ambiguous_parameter_names() -> None:
    client = HttpClient("crm_api", hook_factory=lambda _conn_id, _method: Hook([]))
    with pytest.raises(JobConfigurationError, match="must be different"):
        list(
            client.iter_offset_items(
                "/customers",
                item_key="items",
                page_param="page",
                page_size_param="page",
            )
        )


def test_request_rejects_absolute_or_ambiguous_payload_configuration() -> None:
    client = HttpClient("crm", hook_factory=lambda _conn_id, _method: Hook(Response(200, {})))

    with pytest.raises(JobConfigurationError, match="relative"):
        client.request_json("GET", "https://other.invalid/customers")
    with pytest.raises(JobConfigurationError, match="relative"):
        client.request_json("GET", "//other.invalid/customers")
    with pytest.raises(JobConfigurationError, match="whitespace"):
        client.request_json("GET", " /customers ")
    with pytest.raises(JobConfigurationError, match="either data or json_body"):
        client.request_json("POST", "/customers", data="payload", json_body={"id": 1})


@pytest.mark.parametrize("status", [100, 101, 301, 302, 304, 399])
def test_unexpected_informational_or_redirect_status_is_non_retryable(status: int) -> None:
    client = HttpClient(
        "crm",
        hook_factory=lambda _conn_id, _method: Hook([Response(status, {})]),
    )
    with pytest.raises(NonRetryableJobError, match="unexpected HTTP status"):
        client.request_json("GET", "/customers")
