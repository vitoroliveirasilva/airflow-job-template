from pathlib import Path

import pytest

from scripts.bootstrap_project import BootstrapError, bootstrap

PYPROJECT = """\
[project]
name = "airflow-job-template"

[tool.airflow-job-template]
package = "airflow_job_template"
project_slug = "airflow-job-template"
bootstrapped = false
"""


def _template(tmp_path: Path) -> Path:
    (tmp_path / "src/airflow_job_template/jobs").mkdir(parents=True)
    (tmp_path / "src/airflow_job_template/__init__.py").write_text("", encoding="utf-8")
    (tmp_path / "dags").mkdir()
    (tmp_path / "dags/example.py").write_text(
        "from airflow_job_template.runtime import JobSpec\n", encoding="utf-8"
    )
    (tmp_path / "pyproject.toml").write_text(PYPROJECT, encoding="utf-8")
    (tmp_path / ".env").write_text("SECRET=untouched\n", encoding="utf-8")
    return tmp_path


def test_bootstrap_renames_package_and_imports_without_touching_env(
    tmp_path: Path,
) -> None:
    root = _template(tmp_path)
    bootstrap(root, "customer_sync")

    assert (root / "src/customer_sync_airflow").is_dir()
    assert not (root / "src/airflow_job_template").exists()
    assert "customer_sync_airflow" in (root / "dags/example.py").read_text(
        encoding="utf-8"
    )
    pyproject = (root / "pyproject.toml").read_text(encoding="utf-8")
    assert 'package = "customer_sync_airflow"' in pyproject
    assert "bootstrapped = true" in pyproject
    assert (root / ".env").read_text(encoding="utf-8") == "SECRET=untouched\n"


def test_bootstrap_refuses_second_execution(tmp_path: Path) -> None:
    root = _template(tmp_path)
    bootstrap(root, "customer_sync")
    with pytest.raises(BootstrapError, match="already bootstrapped"):
        bootstrap(root, "another_project")
