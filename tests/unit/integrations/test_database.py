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
