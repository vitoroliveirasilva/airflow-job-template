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


def test_bootstrap_renames_package_and_imports_without_touching_env(tmp_path: Path) -> None:
    root = _template(tmp_path)
    bootstrap(root, "customer_sync")

    assert (root / "src/customer_sync_airflow").is_dir()
    assert not (root / "src/airflow_job_template").exists()
    assert "customer_sync_airflow" in (root / "dags/example.py").read_text(encoding="utf-8")
    pyproject = (root / "pyproject.toml").read_text(encoding="utf-8")
    assert 'package = "customer_sync_airflow"' in pyproject
    assert "bootstrapped = true" in pyproject
    assert (root / ".env").read_text(encoding="utf-8") == "SECRET=untouched\n"


def test_bootstrap_refuses_second_execution(tmp_path: Path) -> None:
    root = _template(tmp_path)
    bootstrap(root, "customer_sync")
    with pytest.raises(BootstrapError, match="already bootstrapped"):
        bootstrap(root, "another_project")


def test_bootstrap_does_not_rewrite_its_own_contract_tests(tmp_path: Path) -> None:
    root = _template(tmp_path)
    protected = {
        "scripts/bootstrap_project.py": 'PLACEHOLDER_PACKAGE = "airflow_job_template"\n',
        "tests/unit/scripts/test_bootstrap_project.py": "airflow_job_template\n",
        "tests/unit/scripts/test_new_job.py": "airflow_job_template\n",
        "tests/unit/scripts/test_scripts_production_regressions.py": "airflow_job_template\n",
    }
    for relative, content in protected.items():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")

    bootstrap(root, "customer_sync")

    for relative, content in protected.items():
        assert (root / relative).read_text(encoding="utf-8") == content


def test_bootstrap_updates_tool_metadata_with_noncanonical_spacing(tmp_path: Path) -> None:
    root = _template(tmp_path)
    pyproject = root / "pyproject.toml"
    text = pyproject.read_text(encoding="utf-8")
    text = text.replace('package = "airflow_job_template"', 'package="airflow_job_template"')
    text = text.replace(
        'project_slug = "airflow-job-template"', 'project_slug="airflow-job-template"'
    )
    text = text.replace("bootstrapped = false", "bootstrapped=false")
    pyproject.write_text(text, encoding="utf-8")

    bootstrap(root, "customer_sync")

    updated = pyproject.read_text(encoding="utf-8")
    assert 'package = "customer_sync_airflow"' in updated
    assert 'project_slug = "customer-sync"' in updated
    assert "bootstrapped = true" in updated


def test_bootstrap_rejects_non_boolean_state(tmp_path: Path) -> None:
    root = _template(tmp_path)
    pyproject = root / "pyproject.toml"
    text = pyproject.read_text(encoding="utf-8").replace(
        "bootstrapped = false", 'bootstrapped = "false"'
    )
    pyproject.write_text(text, encoding="utf-8")

    with pytest.raises(BootstrapError, match="must be a boolean"):
        bootstrap(root, "customer_sync")


def test_bootstrap_updates_assignments_when_section_headers_have_comments(tmp_path: Path) -> None:
    root = _template(tmp_path)
    pyproject = root / "pyproject.toml"
    pyproject.write_text(
        PYPROJECT.replace("[project]", "[project] # package metadata").replace(
            "[tool.airflow-job-template]",
            "[tool.airflow-job-template] # template state",
        ),
        encoding="utf-8",
    )

    bootstrap(root, "customer_sync")

    updated = pyproject.read_text(encoding="utf-8")
    assert 'name = "customer-sync"' in updated
    assert 'package = "customer_sync_airflow"' in updated
    assert 'project_slug = "customer-sync"' in updated
    assert "bootstrapped = true" in updated


@pytest.mark.parametrize("slug", ["customer-", "customer_", "a-"])
def test_bootstrap_rejects_slugs_that_produce_invalid_distribution_names(
    tmp_path: Path, slug: str
) -> None:
    root = _template(tmp_path)

    with pytest.raises(BootstrapError, match="end with a letter or number"):
        bootstrap(root, slug)

    assert (root / "src/airflow_job_template").is_dir()
