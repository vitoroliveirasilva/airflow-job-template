import pytest

from airflow_job_template.integrations.http import HttpClient, HttpTimeout
from airflow_job_template.runtime import NonRetryableJobError, RetryableJobError


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
