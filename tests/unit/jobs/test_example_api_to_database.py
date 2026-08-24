from dataclasses import replace

import pytest

from airflow_job_template.jobs.example_api_to_database import run
from airflow_job_template.runtime import JobConfigurationError


class Http:
    def iter_offset_items(self, *args, **kwargs):
        yield {"id": "1", "name": "Ada", "updated_at": "2026-08-21T10:00:00Z"}
        yield {"id": "2", "name": "Grace", "updated_at": "2026-08-21T10:01:00Z"}


class Database:
    def __init__(self):
        self.batches = []

    def executemany(self, sql, rows, *, chunk_size):
        rows = list(rows)
        self.batches.append((sql, rows, chunk_size))
        return len(rows)


def test_api_to_database_batches_parameterized_upsert(job_context) -> None:
    context = replace(job_context, params={"batch_size": 1, "dry_run": False})
    database = Database()

    result = run(context, http_client=Http(), database_client=database)

    assert result.processed == 2
    assert len(database.batches) == 2
    assert all("ON CONFLICT" in sql for sql, _rows, _size in database.batches)
    assert database.batches[0][1][0][0] == "1"


def test_api_to_database_dry_run_does_not_write(job_context) -> None:
    context = replace(job_context, params={"batch_size": 500, "dry_run": True})
    database = Database()
    result = run(context, http_client=Http(), database_client=database)
    assert result.processed == 2
    assert database.batches == []


@pytest.mark.parametrize(
    "params",
    [
        {"batch_size": 3.5, "dry_run": True},
        {"batch_size": True, "dry_run": True},
        {"batch_size": 100, "dry_run": "false"},
    ],
)
def test_job_rejects_unvalidated_runtime_param_types(job_context, params) -> None:
    context = replace(job_context, params=params)
    with pytest.raises(JobConfigurationError):
        run(context, http_client=Http(), database_client=Database())
