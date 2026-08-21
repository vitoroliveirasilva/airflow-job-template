from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DAGS_DIR = ROOT / "dags"
FORBIDDEN_TOP_LEVEL_CALLS = {
    "requests.get",
    "requests.post",
    "requests.put",
    "requests.patch",
    "requests.delete",
    "Variable.get",
    "Connection.get",
}


def _call_name(node: ast.Call) -> str:
    parts = []
    current = node.func
    while isinstance(current, ast.Attribute):
        parts.append(current.attr)
        current = current.value
    if isinstance(current, ast.Name):
        parts.append(current.id)
    return ".".join(reversed(parts))


def test_dag_modules_do_not_call_external_systems_at_top_level() -> None:
    for path in DAGS_DIR.glob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in tree.body:
            for child in ast.walk(node):
                if isinstance(
                    child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)
                ):
                    continue
                if isinstance(child, ast.Call):
                    assert _call_name(child) not in FORBIDDEN_TOP_LEVEL_CALLS, path.name
