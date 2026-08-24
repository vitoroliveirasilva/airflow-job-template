from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
for path in (ROOT, SRC):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from airflow_job_template.runtime import JobRunContext  # noqa: E402


@pytest.fixture
def job_context() -> JobRunContext:
    return JobRunContext(
        dag_id="unit_test_job",
        task_id="execute",
        run_id="manual__unit-test",
        try_number=1,
        logical_date=datetime(2026, 8, 21, tzinfo=UTC),
        data_interval_start=None,
        data_interval_end=None,
        params={},
    )
