from pathlib import Path

import pytest

from airflow_job_template.integrations.database import DatabaseClient
from airflow_job_template.integrations.files import atomic_write_bytes, safe_filename
from airflow_job_template.integrations.http import HttpClient
from airflow_job_template.runtime import (
    JobConfigurationError,
    NonRetryableJobError,
    RetryableJobError,
)


class HttpResponse:
    status_code = 200

    def json(self):
        return {"items": []}


class HttpHook:
    def __init__(self, error=None):
        self.error = error
        self.calls = []

    def run(self, endpoint: str, **kwargs):
        self.calls.append((endpoint, kwargs))
        if self.error is not None:
            raise self.error
        return HttpResponse()


def test_http_rejects_pagination_params_owned_by_helper() -> None:
    client = HttpClient("crm", hook_factory=lambda _conn_id, _method: HttpHook())
    with pytest.raises(JobConfigurationError, match="must not redefine"):
        list(
            client.iter_offset_items(
                "/customers",
                item_key="items",
                params={"page": 99},
            )
        )


def test_http_rejects_header_crlf_before_network_call() -> None:
    hook = HttpHook()
    client = HttpClient("crm", hook_factory=lambda _conn_id, _method: hook)
    with pytest.raises(JobConfigurationError, match="CR/LF"):
        client.request_json("GET", "/customers", headers={"X-Trace": "safe\r\nInjected: yes"})
    assert hook.calls == []


def test_http_classifies_wrapped_tls_error_as_non_retryable() -> None:
    class SSLError(RuntimeError):
        pass

    try:
        raise SSLError("certificate verify failed")
    except SSLError as cause:
        wrapped = RuntimeError("provider wrapper")
        wrapped.__cause__ = cause

    client = HttpClient("crm", hook_factory=lambda _conn_id, _method: HttpHook(wrapped))
    with pytest.raises(NonRetryableJobError):
        client.request_json("GET", "/customers")


def test_clients_reject_surrounding_whitespace_in_connection_ids() -> None:
    with pytest.raises(JobConfigurationError, match="surrounding whitespace"):
        HttpClient(" crm ")
    with pytest.raises(JobConfigurationError, match="surrounding whitespace"):
        DatabaseClient(" db ")


class Cursor:
    def __init__(self):
        self.execute_args = None
        self.closed = False

    def execute(self, *args):
        self.execute_args = args

    def executemany(self, sql, rows):
        return None

    def fetchmany(self, size):
        return []

    def close(self):
        self.closed = True


class Connection:
    def __init__(self, cursor):
        self._cursor = cursor
        self.closed = False

    def cursor(self):
        return self._cursor

    def commit(self):
        return None

    def rollback(self):
        return None

    def close(self):
        self.closed = True


class DbHook:
    def __init__(self, connection):
        self.connection = connection

    def get_conn(self):
        return self.connection


def test_database_execute_omits_optional_parameter_argument_when_absent() -> None:
    cursor = Cursor()
    connection = Connection(cursor)
    client = DatabaseClient("db", hook_factory=lambda _conn_id: DbHook(connection))
    client.execute("VACUUM")
    assert cursor.execute_args == ("VACUUM",)


def test_database_rejects_invalid_batch_iterable_before_opening_connection() -> None:
    opened = False

    def hook_factory(_conn_id):
        nonlocal opened
        opened = True
        return DbHook(Connection(Cursor()))

    client = DatabaseClient("db", hook_factory=hook_factory)
    with pytest.raises(JobConfigurationError, match="iterable of parameter sets"):
        client.executemany("INSERT INTO t VALUES (%s)", "not rows")
    assert opened is False


def test_database_classifies_wrapped_operational_error_as_retryable() -> None:
    class OperationalError(RuntimeError):
        pass

    class FailingCursor(Cursor):
        def execute(self, *args):
            try:
                raise OperationalError("connection reset")
            except OperationalError as cause:
                raise RuntimeError("driver wrapper") from cause

    connection = Connection(FailingCursor())
    client = DatabaseClient("db", hook_factory=lambda _conn_id: DbHook(connection))
    with pytest.raises(RetryableJobError):
        client.execute("SELECT 1")


@pytest.mark.parametrize("filename", ["CON", "con.txt", "LPT1.log", "aux.json", "report."])
def test_safe_filename_rejects_nonportable_windows_names(filename: str) -> None:
    with pytest.raises(JobConfigurationError):
        safe_filename(filename)


def test_atomic_write_validates_data_and_file_mode(tmp_path: Path) -> None:
    with pytest.raises(JobConfigurationError, match="data must be bytes"):
        atomic_write_bytes(tmp_path / "payload.bin", "text")
    with pytest.raises(JobConfigurationError, match="mode"):
        atomic_write_bytes(tmp_path / "payload.bin", b"data", mode=True)
