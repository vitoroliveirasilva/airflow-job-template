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


def _direct_function_definitions(
    nodes: list[ast.stmt],
) -> dict[str, ast.FunctionDef | ast.AsyncFunctionDef]:
    return {
        node.name: node
        for node in nodes
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
    }


def _decorator_call_name(
    decorator: ast.expr,
    aliases: dict[str, str],
) -> str:
    target = decorator.func if isinstance(decorator, ast.Call) else decorator
    fake_call = ast.Call(func=target, args=[], keywords=[])
    return _canonical_call_name(fake_call, aliases)


def _is_task_function(
    node: ast.FunctionDef | ast.AsyncFunctionDef,
    aliases: dict[str, str],
) -> bool:
    for decorator in node.decorator_list:
        name = _decorator_call_name(decorator, aliases)
        if name == "airflow.sdk.task" or name.startswith("airflow.sdk.task."):
            return True
    return False


class _ParseTimeCallVisitor(ast.NodeVisitor):
    """
    Visit expressions executed while a DAG module constructs its graph.

    Besides module-level expressions, the visitor follows local Python functions when they are
    called from parse-time code. This matters for ``dag = workflow()``: an ``@dag`` function body
    executes while the graph is built, whereas nested ``@task`` function bodies execute later on a
    worker and must remain outside the parse-time scan.
    """

    def __init__(self, tree: ast.Module, aliases: dict[str, str]) -> None:
        self.calls: list[ast.Call] = []
        self.aliases = aliases
        self._scope_stack = [_direct_function_definitions(tree.body)]
        self._active_functions: set[int] = set()

    def visit_Call(self, node: ast.Call) -> None:
        self.calls.append(node)
        self.generic_visit(node)

        name = _call_name(node)
        if "." in name or not name:
            return
        function = self._resolve_local_function(name)
        if function is None or _is_task_function(function, self.aliases):
            return
        self._visit_executed_function(function)

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            local_name = alias.asname or alias.name.split(".", 1)[0]
            self.aliases[local_name] = alias.name

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        if node.module is None:
            return
        for alias in node.names:
            if alias.name == "*":
                continue
            local_name = alias.asname or alias.name
            self.aliases[local_name] = f"{node.module}.{alias.name}"

    def _resolve_local_function(self, name: str) -> ast.FunctionDef | ast.AsyncFunctionDef | None:
        for scope in reversed(self._scope_stack):
            if name in scope:
                return scope[name]
        return None

    def _visit_definition_metadata(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
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

    def _visit_executed_function(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        node_id = id(node)
        if node_id in self._active_functions:
            return
        self._active_functions.add(node_id)
        previous_aliases = self.aliases.copy()
        self._scope_stack.append(_direct_function_definitions(node.body))
        try:
            for statement in node.body:
                self.visit(statement)
        finally:
            self.aliases = previous_aliases
            self._scope_stack.pop()
            self._active_functions.remove(node_id)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._visit_definition_metadata(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._visit_definition_metadata(node)

    def visit_Lambda(self, node: ast.Lambda) -> None:
        # Lambda bodies execute only when called; local lambda call analysis is intentionally
        # outside this small static guard.
        return


def _parse_time_call_names(source: str, *, filename: str = "<unknown>") -> list[str]:
    tree = ast.parse(source, filename=filename)
    aliases = _import_aliases(tree)
    visitor = _ParseTimeCallVisitor(tree, aliases)
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


def test_dag_factory_body_is_parse_time_but_nested_task_body_is_not() -> None:
    source = """
from airflow.sdk import Variable, dag, task

@dag
def workflow():
    Variable.get("bad_graph_build_lookup")

    @task
    def execute():
        return Variable.get("valid_runtime_lookup")

    execute()

dag = workflow()
"""
    names = _parse_time_call_names(source)
    assert names.count("airflow.sdk.Variable.get") == 1


def test_parse_time_local_helper_calls_are_followed() -> None:
    source = """
from airflow.sdk import Variable, dag

def configure_graph():
    return Variable.get("bad_helper_lookup")

@dag
def workflow():
    configure_graph()

dag = workflow()
"""
    assert "airflow.sdk.Variable.get" in _parse_time_call_names(source)


def test_parse_time_imports_inside_dag_factories_cannot_bypass_detection() -> None:
    source = """
from airflow.sdk import dag

@dag
def workflow():
    import requests as req
    from airflow.sdk import Variable as RuntimeVariable

    req.get("https://example.invalid")
    RuntimeVariable.get("bad_parse_lookup")

dag = workflow()
"""
    names = set(_parse_time_call_names(source))
    assert "requests.get" in names
    assert "airflow.sdk.Variable.get" in names
