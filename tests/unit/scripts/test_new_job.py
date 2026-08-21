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
    assert "schedule=None" in (root / "dags/customer_sync.py").read_text(encoding="utf-8")


def test_scaffold_refuses_overwrite(tmp_path: Path) -> None:
    root = _bootstrapped_template(tmp_path)
    create_job(root, "customer_sync", "simple")
    with pytest.raises(ScaffoldError, match="already exists"):
        create_job(root, "customer_sync", "simple")


def test_workflow_scaffold_keeps_schedule_explicit(tmp_path: Path) -> None:
    root = _bootstrapped_template(tmp_path)
    create_job(root, "billing_pipeline", "workflow")
    text = (root / "dags/billing_pipeline.py").read_text(encoding="utf-8")
    assert "@dag(schedule=DAG_SCHEDULE, **DAG_KWARGS)" in text


def test_isolated_scaffold_uses_native_external_python(tmp_path: Path) -> None:
    root = _bootstrapped_template(tmp_path)
    create_job(root, "portal_update", "isolated")
    text = (root / "dags/portal_update.py").read_text(encoding="utf-8")
    assert "@task.external_python" in text
    assert "TaskPolicy(retries=0)" in text
    assert "@dag(schedule=DAG_SCHEDULE, **DAG_KWARGS)" in text


@pytest.mark.parametrize("name", ["class", "import", "async", "await"])
def test_scaffold_rejects_python_keywords(tmp_path: Path, name: str) -> None:
    root = _bootstrapped_template(tmp_path)
    with pytest.raises(ScaffoldError, match="reserved Python keyword"):
        create_job(root, name, "simple")


def test_scaffold_rejects_corrupted_package_path(tmp_path: Path) -> None:
    root = _bootstrapped_template(tmp_path)
    pyproject = root / "pyproject.toml"
    text = pyproject.read_text(encoding="utf-8")
    text = text.replace('package = "billing_airflow"', 'package = "../../outside"')
    pyproject.write_text(text, encoding="utf-8")

    with pytest.raises(ScaffoldError, match="valid Python package identifier"):
        create_job(root, "customer_sync", "simple")

    assert not (root.parent / "outside" / "jobs" / "customer_sync").exists()


def test_scaffold_rejects_non_boolean_bootstrap_state(tmp_path: Path) -> None:
    root = _bootstrapped_template(tmp_path)
    pyproject = root / "pyproject.toml"
    text = pyproject.read_text(encoding="utf-8")
    text = text.replace("bootstrapped = true", 'bootstrapped = "true"')
    pyproject.write_text(text, encoding="utf-8")

    with pytest.raises(ScaffoldError, match="must be a boolean"):
        create_job(root, "customer_sync", "simple")
