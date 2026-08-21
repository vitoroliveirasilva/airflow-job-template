"""Small DB-API helper that resolves provider hooks only at task runtime"""

from __future__ import annotations

import logging
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
        if not isinstance(conn_id, str) or not conn_id.strip():
            raise JobConfigurationError("conn_id must be a non-blank string")
        if hook_factory is not None and not callable(hook_factory):
            raise JobConfigurationError("hook_factory must be callable")
        self.conn_id = conn_id
        self._hook_factory = hook_factory or _default_hook_factory

    def get_hook(self) -> _DbHook:
        """Expose the provider hook for advanced cases instead of hiding Airflow features"""

        return self._hook_factory(self.conn_id)

    @contextmanager
    def connection(self) -> Iterator[_Connection]:
        """Yield a transaction while preserving the primary failure during cleanup"""

        connection = self.get_hook().get_conn()
        failed = False
        try:
            yield connection
            connection.commit()
        except BaseException:
            failed = True
            try:
                connection.rollback()
            except Exception:
                logging.getLogger(__name__).warning(
                    "database rollback failed for conn_id=%r; preserving primary failure",
                    self.conn_id,
                )
            raise
        finally:
            try:
                connection.close()
            except Exception:
                detail = "preserving primary failure" if failed else "commit already completed"
                logging.getLogger(__name__).warning(
                    "database close failed for conn_id=%r; %s",
                    self.conn_id,
                    detail,
                )

    @contextmanager
    def _cursor(self, connection: _Connection) -> Iterator[_Cursor]:
        """Scope a cursor without letting cleanup hide the operation that failed"""

        cursor = connection.cursor()
        failed = False
        try:
            yield cursor
        except BaseException:
            failed = True
            raise
        finally:
            try:
                cursor.close()
            except Exception as exc:
                if not failed:
                    raise RetryableJobError(
                        f"database cursor cleanup failed for conn_id={self.conn_id!r}"
                    ) from exc
                logging.getLogger(__name__).warning(
                    "database cursor cleanup failed for conn_id=%r; preserving primary failure",
                    self.conn_id,
                )

    def execute(self, sql: str, parameters: Any = None) -> None:
        """Execute one parameterized statement inside a transaction"""

        if not isinstance(sql, str) or not sql.strip():
            raise JobConfigurationError("sql must be a non-blank string")
        try:
            with self.connection() as connection, self._cursor(connection) as cursor:
                cursor.execute(sql, parameters)
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

        if not isinstance(sql, str) or not sql.strip():
            raise JobConfigurationError("sql must be a non-blank string")
        if isinstance(chunk_size, bool) or not isinstance(chunk_size, int) or chunk_size < 1:
            raise JobConfigurationError("chunk_size must be an integer >= 1")

        processed = 0
        try:
            with self.connection() as connection, self._cursor(connection) as cursor:
                for batch in _batched(rows, chunk_size):
                    cursor.executemany(sql, batch)
                    processed += len(batch)
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

        if not isinstance(sql, str) or not sql.strip():
            raise JobConfigurationError("sql must be a non-blank string")
        if isinstance(fetch_size, bool) or not isinstance(fetch_size, int) or fetch_size < 1:
            raise JobConfigurationError("fetch_size must be an integer >= 1")

        connection: _Connection | None = None
        failed = False
        try:
            connection = self.get_hook().get_conn()
            with self._cursor(connection) as cursor:
                cursor.execute(sql, parameters)
                while True:
                    rows = cursor.fetchmany(fetch_size)
                    if not rows:
                        break
                    yield from rows
        except JobConfigurationError:
            failed = True
            raise
        except Exception as exc:
            failed = True
            _raise_db_error(exc, operation="fetch", conn_id=self.conn_id)
        except BaseException:
            failed = True
            raise
        finally:
            if connection is not None:
                try:
                    connection.close()
                except Exception as exc:
                    if not failed:
                        raise RetryableJobError(
                            f"database stream cleanup failed for conn_id={self.conn_id!r}"
                        ) from exc
                    logging.getLogger(__name__).warning(
                        "database stream cleanup failed for conn_id=%r; preserving primary failure",
                        self.conn_id,
                    )

    def fetch_all(
        self,
        sql: str,
        parameters: Any = None,
        *,
        max_rows: int = 10_000,
        fetch_size: int = 1_000,
    ) -> list[Any]:
        """Collect a deliberately bounded result set for genuinely small queries"""

        if isinstance(max_rows, bool) or not isinstance(max_rows, int) or max_rows < 1:
            raise JobConfigurationError("max_rows must be an integer >= 1")
        result: list[Any] = []
        for row in self.iter_rows(sql, parameters, fetch_size=fetch_size):
            result.append(row)
            if len(result) > max_rows:
                raise JobConfigurationError(
                    f"query exceeded max_rows={max_rows}; stream/chunk the result instead"
                )
        return result
