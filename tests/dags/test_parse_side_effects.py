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


def _parse_time_calls(source: str, *, filename: str = "<unknown>") -> list[ast.Call]:
    tree = ast.parse(source, filename=filename)
    visitor = _ParseTimeCallVisitor()
    visitor.visit(tree)
    return visitor.calls


def test_dag_modules_do_not_call_external_systems_at_top_level() -> None:
    for path in DAGS_DIR.glob("*.py"):
        source = path.read_text(encoding="utf-8")
        for call in _parse_time_calls(source, filename=str(path)):
            assert _call_name(call) not in FORBIDDEN_TOP_LEVEL_CALLS, path.name


def test_runtime_variable_lookup_is_not_mistaken_for_parse_time_io() -> None:
    source = """
from airflow.sdk import Variable, task

@task
def execute():
    return Variable.get(\"batch_size\")
"""
    assert all(_call_name(call) != "Variable.get" for call in _parse_time_calls(source))


def test_decorator_expression_is_still_considered_parse_time() -> None:
    source = """
from airflow.sdk import Variable

@decorator(value=Variable.get(\"bad_parse_lookup\"))
def execute():
    return 1
"""
    assert "Variable.get" in {_call_name(call) for call in _parse_time_calls(source)}
