from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DAGS_DIR = ROOT / "dags"
FORBIDDEN_TOP_LEVEL_CALLS = {
    "airflow.models.Connection.get",
    "airflow.models.Variable.get",
    "airflow.sdk.BaseHook.get_connection",
    "airflow.sdk.BaseHook.get_hook",
    "airflow.sdk.Connection.get",
    "airflow.sdk.Variable.get",
    "httpx.delete",
    "httpx.get",
    "httpx.patch",
    "httpx.post",
    "httpx.put",
    "httpx.request",
    "requests.delete",
    "requests.get",
    "requests.patch",
    "requests.post",
    "requests.put",
    "requests.request",
    "urllib.request.urlopen",
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


def _import_aliases(tree: ast.Module) -> dict[str, str]:
    aliases: dict[str, str] = {}
    for node in tree.body:
        if isinstance(node, ast.Import):
            for alias in node.names:
                local_name = alias.asname or alias.name.split(".", 1)[0]
                aliases[local_name] = alias.name
        elif isinstance(node, ast.ImportFrom) and node.module:
            for alias in node.names:
                if alias.name == "*":
                    continue
                local_name = alias.asname or alias.name
                aliases[local_name] = f"{node.module}.{alias.name}"
    return aliases


def _canonical_call_name(node: ast.Call, aliases: dict[str, str]) -> str:
    name = _call_name(node)
    if not name:
        return name
    head, separator, tail = name.partition(".")
    canonical_head = aliases.get(head, head)
    return canonical_head + (separator + tail if separator else "")


class _ParseTimeCallVisitor(ast.NodeVisitor):
    """Visit expressions executed while a DAG module is imported, not task/function bodies"""

    def __init__(self) -> None:
        self.calls: list[ast.Call] = []

    def visit_Call(self, node: ast.Call) -> None:
        self.calls.append(node)
        self.generic_visit(node)

    def _visit_function_definition(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        # Decorators/defaults/annotations are evaluated when the function is defined
        # The function body is not.
        for decorator in node.decorator_list:
            self.visit(decorator)
        for default in (*node.args.defaults, *node.args.kw_defaults):
            if default is not None:
                self.visit(default)
        for arg in (*node.args.posonlyargs, *node.args.args, *node.args.kwonlyargs):
            if arg.annotation is not None:
                self.visit(arg.annotation)
        if node.args.vararg and node.args.vararg.annotation is not None:
            self.visit(node.args.vararg.annotation)
        if node.args.kwarg and node.args.kwarg.annotation is not None:
            self.visit(node.args.kwarg.annotation)
        if node.returns is not None:
            self.visit(node.returns)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._visit_function_definition(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._visit_function_definition(node)

    def visit_Lambda(self, node: ast.Lambda) -> None:
        # Lambda defaults do not exist; its body executes only when called.
        return


def _parse_time_call_names(source: str, *, filename: str = "<unknown>") -> list[str]:
    tree = ast.parse(source, filename=filename)
    aliases = _import_aliases(tree)
    visitor = _ParseTimeCallVisitor()
    visitor.visit(tree)
    return [_canonical_call_name(call, aliases) for call in visitor.calls]


def test_dag_modules_do_not_call_external_systems_at_top_level() -> None:
    for path in DAGS_DIR.glob("*.py"):
        source = path.read_text(encoding="utf-8")
        for call_name in _parse_time_call_names(source, filename=str(path)):
            assert call_name not in FORBIDDEN_TOP_LEVEL_CALLS, path.name


def test_runtime_variable_lookup_is_not_mistaken_for_parse_time_io() -> None:
    source = """
from airflow.sdk import Variable, task

@task
def execute():
    return Variable.get("batch_size")
"""
    assert "airflow.sdk.Variable.get" not in _parse_time_call_names(source)


def test_decorator_expression_is_still_considered_parse_time() -> None:
    source = """
from airflow.sdk import Variable

@decorator(value=Variable.get("bad_parse_lookup"))
def execute():
    return 1
"""
    assert "airflow.sdk.Variable.get" in _parse_time_call_names(source)


def test_aliases_do_not_bypass_parse_time_external_io_detection() -> None:
    source = """
import requests as req
from airflow.sdk import Variable as V

req.get("https://example.invalid")
V.get("bad_parse_lookup")
"""
    names = set(_parse_time_call_names(source))
    assert "requests.get" in names
    assert "airflow.sdk.Variable.get" in names
