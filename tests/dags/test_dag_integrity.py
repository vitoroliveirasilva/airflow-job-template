from __future__ import annotations

import importlib.util
import re
import sys
from pathlib import Path

import pytest

if sys.platform == "win32":
    pytest.skip(
        "Airflow DAG integrity tests require a POSIX environment; use WSL2/Linux or CI",
        allow_module_level=True,
    )

pytest.importorskip("airflow")

ROOT = Path(__file__).resolve().parents[2]
DAGS_DIR = ROOT / "dags"
TAG_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")


def _load_dags():
    loaded = []
    for path in sorted(DAGS_DIR.glob("*.py")):
        spec = importlib.util.spec_from_file_location(f"integrity_{path.stem}", path)
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        dag = getattr(module, "dag", None)
        assert dag is not None, f"{path.name} must expose global `dag`"
        loaded.append((path, dag))
    return loaded


def test_all_dags_import_and_have_unique_ids() -> None:
    loaded = _load_dags()
    dag_ids = [dag.dag_id for _path, dag in loaded]
    assert len(dag_ids) == len(set(dag_ids))


def test_dag_safety_conventions() -> None:
    for path, dag in _load_dags():
        assert dag.catchup is False, path.name
        assert dag.start_date is not None, path.name
        assert dag.start_date.tzinfo is not None, path.name
        assert len(dag.tags) <= 10, path.name
        assert all(TAG_RE.fullmatch(tag) for tag in dag.tags), path.name
        task_ids = [task.task_id for task in dag.tasks]
        assert len(task_ids) == len(set(task_ids)), path.name
        assert all(task.execution_timeout is not None for task in dag.tasks), path.name
