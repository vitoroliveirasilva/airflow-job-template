"""Small DB-API helper that resolves provider hooks only at task runtime"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Sequence
from contextlib import contextmanager
from typing import Any, Protocol

from airflow_job_template.runtime.errors import (
    JobConfigurationError,
    NonRetryableJobError,
    RetryableJobError,
)


class _Cursor(Protocol):
    def execute(self, sql: str, parameters: Any = None) -> Any: ...
    def executemany(self, sql: str, seq_of_parameters: Sequence[Any]) -> Any: ...
    def fetchmany(self, size: int) -> Sequence[Any]: ...
    def close(self) -> Any: ...


class _Connection(Protocol):
    def cursor(self) -> _Cursor: ...
    def commit(self) -> Any: ...
    def rollback(self) -> Any: ...
    def close(self) -> Any: ...


class _DbHook(Protocol):
    def get_conn(self) -> _Connection: ...


HookFactory = Callable[[str], _DbHook]


def _default_hook_factory(conn_id: str) -> _DbHook:
    try:
        from airflow.sdk import BaseHook
        from airflow.sdk.exceptions import AirflowNotFoundException
    except ImportError as exc:
        raise JobConfigurationError("Airflow Task SDK is not installed") from exc

    try:
        hook = BaseHook.get_hook(conn_id)
    except AirflowNotFoundException as exc:
        raise JobConfigurationError(f"Airflow Connection {conn_id!r} was not found") from exc
    if not hasattr(hook, "get_conn"):
        raise JobConfigurationError(f"Connection {conn_id!r} does not expose a DB-API hook")
    return hook


def _raise_db_error(exc: Exception, *, operation: str, conn_id: str) -> None:
    """Map portable PEP-249 error classes and leave unknown driver errors untouched"""

    class_names = {cls.__name__ for cls in type(exc).__mro__}
    message = f"database {operation} failed for conn_id={conn_id!r}"
    if class_names & {"OperationalError", "InterfaceError"}:
        raise RetryableJobError(message) from exc
    if class_names & {"ProgrammingError", "IntegrityError", "DataError", "NotSupportedError"}:
        raise NonRetryableJobError(message) from exc
    raise exc


def _batched(rows: Iterable[Any], size: int) -> Iterator[list[Any]]:
    batch: list[Any] = []
    for row in rows:
        batch.append(row)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


class DatabaseClient:
    """DB-API operations with explicit transaction boundaries and bounded fetching"""

    def __init__(self, conn_id: str, *, hook_factory: HookFactory | None = None) -> None:
        if not conn_id.strip():
            raise JobConfigurationError("conn_id cannot be blank")
        self.conn_id = conn_id
        self._hook_factory = hook_factory or _default_hook_factory

    def get_hook(self) -> _DbHook:
        """Expose the provider hook for advanced cases instead of hiding Airflow features"""

        return self._hook_factory(self.conn_id)

    @contextmanager
    def connection(self) -> Iterator[_Connection]:
        """Yield a connection and commit/rollback exactly once around the caller's work"""

        connection = self.get_hook().get_conn()
        try:
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def execute(self, sql: str, parameters: Any = None) -> None:
        """Execute one parameterized statement inside a transaction"""

        if not sql.strip():
            raise JobConfigurationError("sql cannot be blank")
        try:
            with self.connection() as connection:
                cursor = connection.cursor()
                try:
                    cursor.execute(sql, parameters)
                finally:
                    cursor.close()
        except JobConfigurationError:
            raise
        except Exception as exc:
            _raise_db_error(exc, operation="execute", conn_id=self.conn_id)

    def executemany(
        self,
        sql: str,
        rows: Iterable[Any],
        *,
        chunk_size: int = 1_000,
    ) -> int:
        """Batch many parameter sets in one transaction without materializing the whole input"""

        if not sql.strip():
            raise JobConfigurationError("sql cannot be blank")
        if chunk_size < 1:
            raise JobConfigurationError("chunk_size must be >= 1")

        processed = 0
        try:
            with self.connection() as connection:
                cursor = connection.cursor()
                try:
                    for batch in _batched(rows, chunk_size):
                        cursor.executemany(sql, batch)
                        processed += len(batch)
                finally:
                    cursor.close()
        except JobConfigurationError:
            raise
        except Exception as exc:
            _raise_db_error(exc, operation="batch", conn_id=self.conn_id)
        return processed

    def iter_rows(
        self,
        sql: str,
        parameters: Any = None,
        *,
        fetch_size: int = 1_000,
    ) -> Iterator[Any]:
        """Stream rows in bounded batches while keeping the connection scoped to iteration"""

        if not sql.strip():
            raise JobConfigurationError("sql cannot be blank")
        if fetch_size < 1:
            raise JobConfigurationError("fetch_size must be >= 1")

        connection: _Connection | None = None
        cursor: _Cursor | None = None
        try:
            connection = self.get_hook().get_conn()
            cursor = connection.cursor()
            cursor.execute(sql, parameters)
            while True:
                rows = cursor.fetchmany(fetch_size)
                if not rows:
                    break
                yield from rows
        except JobConfigurationError:
            raise
        except Exception as exc:
            _raise_db_error(exc, operation="fetch", conn_id=self.conn_id)
        finally:
            if cursor is not None:
                try:
                    cursor.close()
                finally:
                    if connection is not None:
                        connection.close()
            elif connection is not None:
                connection.close()

    def fetch_all(
        self,
        sql: str,
        parameters: Any = None,
        *,
        max_rows: int = 10_000,
        fetch_size: int = 1_000,
    ) -> list[Any]:
        """Collect a deliberately bounded result set for genuinely small queries"""

        if max_rows < 1:
            raise JobConfigurationError("max_rows must be >= 1")
        result: list[Any] = []
        for row in self.iter_rows(sql, parameters, fetch_size=fetch_size):
            result.append(row)
            if len(result) > max_rows:
                raise JobConfigurationError(
                    f"query exceeded max_rows={max_rows}; stream/chunk the result instead"
                )
        return result
