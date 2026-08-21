import pytest

from airflow_job_template.integrations.database import DatabaseClient
from airflow_job_template.runtime import (
    JobConfigurationError,
    NonRetryableJobError,
    RetryableJobError,
)


class OperationalError(RuntimeError):
    pass


class ProgrammingError(RuntimeError):
    pass


class Cursor:
    def __init__(self, rows=(), fail_execute=False, failure_type=OperationalError):
        self.rows = list(rows)
        self.fail_execute = fail_execute
        self.failure_type = failure_type
        self.closed = False
        self.executed = []
        self.executed_many = []

    def execute(self, sql, parameters=None):
        if self.fail_execute:
            raise self.failure_type("db failure")
        self.executed.append((sql, parameters))

    def executemany(self, sql, rows):
        self.executed_many.append((sql, list(rows)))

    def fetchmany(self, size):
        batch = self.rows[:size]
        self.rows = self.rows[size:]
        return batch

    def close(self):
        self.closed = True


class Connection:
    def __init__(self, cursor):
        self._cursor = cursor
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def cursor(self):
        return self._cursor

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        self.closed = True


class Hook:
    def __init__(self, connection):
        self.connection = connection

    def get_conn(self):
        return self.connection


def test_executemany_batches_and_commits() -> None:
    cursor = Cursor()
    connection = Connection(cursor)
    client = DatabaseClient("target_db", hook_factory=lambda _conn_id: Hook(connection))

    processed = client.executemany("INSERT INTO t VALUES (%s)", [(1,), (2,), (3,)], chunk_size=2)

    assert processed == 3
    assert [len(rows) for _sql, rows in cursor.executed_many] == [2, 1]
    assert connection.committed is True
    assert connection.closed is True


def test_execute_rolls_back_and_classifies_transient_failure() -> None:
    cursor = Cursor(fail_execute=True)
    connection = Connection(cursor)
    client = DatabaseClient("target_db", hook_factory=lambda _conn_id: Hook(connection))

    with pytest.raises(RetryableJobError):
        client.execute("UPDATE t SET value=%s", (1,))

    assert connection.rolled_back is True
    assert connection.closed is True


def test_fetch_all_refuses_unbounded_materialization() -> None:
    cursor = Cursor(rows=[(1,), (2,), (3,)])
    connection = Connection(cursor)
    client = DatabaseClient("source_db", hook_factory=lambda _conn_id: Hook(connection))

    with pytest.raises(JobConfigurationError, match="max_rows"):
        client.fetch_all("SELECT id FROM t", max_rows=2, fetch_size=1)
    assert connection.closed is True


def test_programming_error_is_non_retryable() -> None:
    cursor = Cursor(fail_execute=True, failure_type=ProgrammingError)
    connection = Connection(cursor)
    client = DatabaseClient("target_db", hook_factory=lambda _conn_id: Hook(connection))

    with pytest.raises(NonRetryableJobError):
        client.execute("BROKEN SQL")
    assert connection.rolled_back is True
    assert connection.closed is True


def test_iter_rows_classifies_connection_failure_and_has_nothing_to_leak() -> None:
    class FailingHook:
        def get_conn(self):
            raise OperationalError("database unavailable")

    client = DatabaseClient("source_db", hook_factory=lambda _conn_id: FailingHook())
    with pytest.raises(RetryableJobError):
        list(client.iter_rows("SELECT id FROM t"))


def test_iter_rows_closes_connection_when_cursor_creation_fails() -> None:
    class CursorFailureConnection(Connection):
        def cursor(self):
            raise ProgrammingError("cursor configuration invalid")

    connection = CursorFailureConnection(Cursor())
    client = DatabaseClient("source_db", hook_factory=lambda _conn_id: Hook(connection))

    with pytest.raises(NonRetryableJobError):
        list(client.iter_rows("SELECT id FROM t"))
    assert connection.closed is True


def test_database_client_rejects_invalid_runtime_configuration() -> None:
    with pytest.raises(JobConfigurationError, match="conn_id"):
        DatabaseClient(None)
    with pytest.raises(JobConfigurationError, match="hook_factory"):
        DatabaseClient("db", hook_factory="not-callable")


def test_database_methods_reject_boolean_sizes() -> None:
    client = DatabaseClient("db", hook_factory=lambda _conn_id: Hook(Connection(Cursor())))
    with pytest.raises(JobConfigurationError, match="chunk_size"):
        client.executemany("INSERT INTO t VALUES (%s)", [], chunk_size=True)
    with pytest.raises(JobConfigurationError, match="fetch_size"):
        list(client.iter_rows("SELECT 1", fetch_size=True))
    with pytest.raises(JobConfigurationError, match="max_rows"):
        client.fetch_all("SELECT 1", max_rows=True)


def test_rollback_failure_does_not_mask_primary_database_error() -> None:
    class RollbackFailureConnection(Connection):
        def rollback(self):
            self.rolled_back = True
            raise RuntimeError("rollback cleanup failed")

    cursor = Cursor(fail_execute=True, failure_type=OperationalError)
    connection = RollbackFailureConnection(cursor)
    client = DatabaseClient("target_db", hook_factory=lambda _conn_id: Hook(connection))

    with pytest.raises(RetryableJobError) as exc_info:
        client.execute("UPDATE t SET value=%s", (1,))

    assert isinstance(exc_info.value.__cause__, OperationalError)
    assert connection.rolled_back is True
    assert connection.closed is True


def test_close_failure_does_not_mask_primary_database_error() -> None:
    class CloseFailureConnection(Connection):
        def close(self):
            self.closed = True
            raise RuntimeError("close cleanup failed")

    cursor = Cursor(fail_execute=True, failure_type=ProgrammingError)
    connection = CloseFailureConnection(cursor)
    client = DatabaseClient("target_db", hook_factory=lambda _conn_id: Hook(connection))

    with pytest.raises(NonRetryableJobError) as exc_info:
        client.execute("BROKEN SQL")

    assert isinstance(exc_info.value.__cause__, ProgrammingError)
    assert connection.closed is True


def test_close_failure_after_success_does_not_retry_committed_write() -> None:
    class CloseFailureConnection(Connection):
        def close(self):
            self.closed = True
            raise RuntimeError("close cleanup failed")

    cursor = Cursor()
    connection = CloseFailureConnection(cursor)
    client = DatabaseClient("target_db", hook_factory=lambda _conn_id: Hook(connection))

    client.execute("UPDATE t SET value=%s", (1,))

    assert connection.committed is True
    assert connection.closed is True


def test_cursor_cleanup_failure_rolls_back_before_retry() -> None:
    class CloseFailureCursor(Cursor):
        def close(self):
            self.closed = True
            raise RuntimeError("cursor cleanup failed")

    cursor = CloseFailureCursor()
    connection = Connection(cursor)
    client = DatabaseClient("target_db", hook_factory=lambda _conn_id: Hook(connection))

    with pytest.raises(RetryableJobError, match="cursor cleanup"):
        client.execute("UPDATE t SET value=%s", (1,))

    assert connection.rolled_back is True
    assert connection.committed is False
    assert connection.closed is True


def test_stream_cleanup_failure_does_not_mask_fetch_error() -> None:
    class FetchFailureCursor(Cursor):
        def fetchmany(self, size):
            raise OperationalError("fetch failed")

        def close(self):
            self.closed = True
            raise RuntimeError("cursor cleanup failed")

    class CloseFailureConnection(Connection):
        def close(self):
            self.closed = True
            raise RuntimeError("connection cleanup failed")

    cursor = FetchFailureCursor()
    connection = CloseFailureConnection(cursor)
    client = DatabaseClient("source_db", hook_factory=lambda _conn_id: Hook(connection))

    with pytest.raises(RetryableJobError) as exc_info:
        list(client.iter_rows("SELECT id FROM t"))

    assert isinstance(exc_info.value.__cause__, OperationalError)
    assert cursor.closed is True
    assert connection.closed is True
