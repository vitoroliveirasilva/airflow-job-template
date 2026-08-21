from pathlib import Path

import pytest

from scripts.bootstrap_project import bootstrap
from scripts.new_job import ScaffoldError, create_job

PYPROJECT = """\
[project]
name = "airflow-job-template"

[tool.airflow-job-template]
package = "airflow_job_template"
project_slug = "airflow-job-template"
bootstrapped = false
"""


def _bootstrapped_template(tmp_path: Path) -> Path:
    (tmp_path / "src/airflow_job_template/jobs").mkdir(parents=True)
    (tmp_path / "src/airflow_job_template/__init__.py").write_text("", encoding="utf-8")
    (tmp_path / "dags").mkdir()
    (tmp_path / "tests/unit/jobs").mkdir(parents=True)
    (tmp_path / "pyproject.toml").write_text(PYPROJECT, encoding="utf-8")
    bootstrap(tmp_path, "billing")
    return tmp_path


def test_simple_scaffold_creates_expected_paths(tmp_path: Path) -> None:
    root = _bootstrapped_template(tmp_path)
    created = create_job(root, "customer_sync", "simple")
    relative = {path.relative_to(root).as_posix() for path in created}
    assert relative == {
        "dags/customer_sync.py",
        "src/billing_airflow/jobs/customer_sync/__init__.py",
        "src/billing_airflow/jobs/customer_sync/job.py",
        "tests/unit/jobs/test_customer_sync.py",
    }
    assert "schedule=None" in (root / "dags/customer_sync.py").read_text(
        encoding="utf-8"
    )


def test_scaffold_refuses_overwrite(tmp_path: Path) -> None:
    root = _bootstrapped_template(tmp_path)
    create_job(root, "customer_sync", "simple")
    with pytest.raises(ScaffoldError, match="already exists"):
        create_job(root, "customer_sync", "simple")


def test_isolated_scaffold_uses_native_external_python(tmp_path: Path) -> None:
    root = _bootstrapped_template(tmp_path)
    create_job(root, "portal_update", "isolated")
    text = (root / "dags/portal_update.py").read_text(encoding="utf-8")
    assert "@task.external_python" in text
    assert "TaskPolicy(retries=0)" in text
