from dataclasses import replace

from airflow_job_template.jobs.example_api_to_database import run


class Http:
    def iter_offset_items(self, *args, **kwargs):
        yield {"id": "1", "name": "Ada", "updated_at": "2026-08-21T10:00:00Z"}


class Database:
    def __init__(self):
        self.calls = 0

    def executemany(self, sql, rows, *, chunk_size):
        self.calls += 1
        return len(list(rows))


def test_api_to_database_defaults_to_dry_run_when_param_is_absent(job_context) -> None:
    database = Database()
    result = run(
        replace(job_context, params={}),
        http_client=Http(),
        database_client=database,
    )
    assert result.processed == 1
    assert database.calls == 0
