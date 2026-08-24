import os
from pathlib import Path

import pytest
from scripts.bootstrap_project import BootstrapError, bootstrap
from scripts.check_secrets import scan
from scripts.new_job import ScaffoldError, create_job

PYPROJECT = """\
[project]
name = "airflow-job-template"

[tool.airflow-job-template]
package = "airflow_job_template"
project_slug = "airflow-job-template"
bootstrapped = false
"""


def _template(root: Path) -> Path:
    (root / "src/airflow_job_template/jobs").mkdir(parents=True)
    (root / "src/airflow_job_template/__init__.py").write_text("", encoding="utf-8")
    (root / "dags").mkdir()
    (root / "dags/example.py").write_text(
        "from airflow_job_template.runtime import JobSpec\n",
        encoding="utf-8",
    )
    (root / "tests/unit/jobs").mkdir(parents=True)
    (root / "pyproject.toml").write_text(PYPROJECT, encoding="utf-8")
    return root


def test_bootstrap_skip_dirs_are_relative_to_repository_not_ancestor(
    tmp_path: Path,
) -> None:
    root = _template(tmp_path / "build" / "repo")
    bootstrap(root, "customer_sync")
    assert "customer_sync_airflow" in (root / "dags/example.py").read_text(encoding="utf-8")


@pytest.mark.skipif(
    os.name == "nt",
    reason="exact POSIX permission bits are not supported on Windows",
)
def test_bootstrap_preserves_permissions_of_rewritten_files(tmp_path: Path) -> None:
    root = _template(tmp_path)
    dag_file = root / "dags/example.py"

    os.chmod(dag_file, 0o640)

    bootstrap(root, "customer_sync")

    assert dag_file.stat().st_mode & 0o777 == 0o640


@pytest.mark.skipif(os.name == "nt", reason="symlink creation is not reliably available on Windows")
def test_bootstrap_refuses_symlinked_pyproject(tmp_path: Path) -> None:
    root = _template(tmp_path / "repo")
    real_project = tmp_path / "outside.toml"
    real_project.write_text(PYPROJECT, encoding="utf-8")
    (root / "pyproject.toml").unlink()
    (root / "pyproject.toml").symlink_to(real_project)
    with pytest.raises(BootstrapError, match="symbolic link"):
        bootstrap(root, "customer_sync")


def test_workflow_scaffold_uses_non_retryable_error_adapter(tmp_path: Path) -> None:
    root = _template(tmp_path)
    bootstrap(root, "billing")
    create_job(root, "billing_pipeline", "workflow")
    dag_text = (root / "dags/billing_pipeline.py").read_text(encoding="utf-8")
    assert "run_with_airflow_error_policy" in dag_text
    assert "run_with_airflow_error_policy(extract)" in dag_text
    assert "run_with_airflow_error_policy(transform, metadata)" in dag_text
    assert "run_with_airflow_error_policy(load, metadata)" in dag_text


@pytest.mark.skipif(os.name == "nt", reason="symlink creation is not reliably available on Windows")
def test_new_job_refuses_symlinked_package_directory(tmp_path: Path) -> None:
    root = _template(tmp_path / "repo")
    bootstrap(root, "billing")
    package_dir = root / "src/billing_airflow"
    outside = tmp_path / "outside_package"
    package_dir.rename(outside)
    package_dir.symlink_to(outside, target_is_directory=True)
    with pytest.raises(ScaffoldError, match="symbolic link"):
        create_job(root, "customer_sync", "simple")


def test_secret_scan_detects_fine_grained_github_pat(tmp_path: Path) -> None:
    token = "github_pat_" + ("A" * 30)
    (tmp_path / "settings.txt").write_text(f"TOKEN={token}\n", encoding="utf-8")
    assert scan(tmp_path) == ["GitHub token: settings.txt"]


@pytest.mark.skipif(os.name == "nt", reason="symlink creation is not reliably available on Windows")
def test_secret_scan_does_not_follow_symlinks_outside_repository(
    tmp_path: Path,
) -> None:
    outside = tmp_path.parent / "outside-secret.txt"
    key = "AK" + "IA" + ("A" * 16)
    outside.write_text(key, encoding="utf-8")
    (tmp_path / "linked.txt").symlink_to(outside)
    assert scan(tmp_path) == []
