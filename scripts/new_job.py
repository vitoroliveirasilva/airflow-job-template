"""Generate a small, explicit Airflow job scaffold for a bootstrapped project"""

from __future__ import annotations

import argparse
import keyword
import os
import re
import sys
import tempfile
import textwrap
import tomllib
from pathlib import Path

TOOL_NAME = "airflow-job-template"
JOB_RE = re.compile(r"^[a-z][a-z0-9_]{1,99}$")
PACKAGE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class ScaffoldError(RuntimeError):
    """Raised when a scaffold cannot be generated safely"""


def validate_job_name(name: str) -> str:
    normalized = name.strip().lower()
    if not JOB_RE.fullmatch(normalized):
        raise ScaffoldError("job name must be 2..100 snake_case characters and start with a letter")
    if keyword.iskeyword(normalized):
        raise ScaffoldError(f"job name {normalized!r} is a reserved Python keyword")
    return normalized


def _state(root: Path) -> tuple[str, bool]:
    pyproject = root / "pyproject.toml"
    if not pyproject.is_file():
        raise ScaffoldError("pyproject.toml not found; run from the repository root")
    try:
        with pyproject.open("rb") as handle:
            data = tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise ScaffoldError(f"cannot read valid pyproject.toml: {exc}") from exc
    try:
        config = data["tool"][TOOL_NAME]
        package = config["package"]
        bootstrapped = config["bootstrapped"]
    except (KeyError, TypeError) as exc:
        raise ScaffoldError(f"missing [tool.{TOOL_NAME}] configuration") from exc

    if (
        not isinstance(package, str)
        or not PACKAGE_RE.fullmatch(package)
        or keyword.iskeyword(package)
    ):
        raise ScaffoldError("configured package must be a valid Python package identifier")
    if not isinstance(bootstrapped, bool):
        raise ScaffoldError("configured bootstrapped flag must be a boolean")
    return package, bootstrapped


def _atomic_create(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        raise ScaffoldError(f"refusing to overwrite existing file: {path}")
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temp_path = Path(temp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, path)
    except Exception:
        temp_path.unlink(missing_ok=True)
        raise


def _template(content: str) -> str:
    return textwrap.dedent(content).lstrip()


def _simple_files(package: str, name: str) -> dict[str, str]:
    return {
        f"dags/{name}.py": _template(
            f'''\
            """Airflow DAG for {name}."""

            from {package}.jobs.{name} import run
            from {package}.runtime import JobSpec, build_single_task_dag

            SPEC = JobSpec(
                dag_id="{name}",
                description="Generated scaffold for {name}; update before scheduling.",
                schedule=None,
                tags=("generated",),
            )

            dag = build_single_task_dag(spec=SPEC, job_callable=run)
            '''
        ),
        f"src/{package}/jobs/{name}/__init__.py": _template(
            """\
            from .job import run

            __all__ = ["run"]
            """
        ),
        f"src/{package}/jobs/{name}/job.py": _template(
            f'''\
            """Business logic for {name}."""

            from {package}.runtime import JobConfigurationError, JobResult, JobRunContext


            def run(context: JobRunContext) -> JobResult | None:
                raise JobConfigurationError(
                    "Implement {name}.run before enabling the DAG schedule"
                )
            '''
        ),
        f"tests/unit/jobs/test_{name}.py": _template(
            f"""\
            import pytest

            from {package}.jobs.{name} import run
            from {package}.runtime import JobConfigurationError, JobRunContext


            def test_placeholder_fails_clearly(job_context: JobRunContext) -> None:
                with pytest.raises(JobConfigurationError, match="Implement {name}"):
                    run(job_context)
            """
        ),
    }


def _workflow_files(package: str, name: str) -> dict[str, str]:
    return {
        f"dags/{name}.py": _template(
            f'''\
            """Explicit workflow DAG for {name}."""

            from airflow.sdk import dag, task

            from {package}.jobs.{name} import extract, load, transform
            from {package}.runtime import JobSpec

            SPEC = JobSpec(
                dag_id="{name}",
                description="Generated workflow for {name}; update before scheduling.",
                schedule=None,
                tags=("generated", "workflow"),
            )


            DAG_KWARGS = SPEC.as_dag_kwargs()
            DAG_SCHEDULE = DAG_KWARGS.pop("schedule")


            @dag(schedule=DAG_SCHEDULE, **DAG_KWARGS)
            def workflow():
                @task(**SPEC.task_policy.as_task_kwargs())
                def extract_task() -> dict[str, int | str]:
                    return extract()

                @task(**SPEC.task_policy.as_task_kwargs())
                def transform_task(metadata: dict[str, int | str]) -> dict[str, int | str]:
                    return transform(metadata)

                @task(**SPEC.task_policy.as_task_kwargs())
                def load_task(metadata: dict[str, int | str]) -> None:
                    load(metadata)

                load_task(transform_task(extract_task()))


            dag = workflow()
            '''
        ),
        f"src/{package}/jobs/{name}/__init__.py": _template(
            """\
            from .job import extract, load, transform

            __all__ = ["extract", "load", "transform"]
            """
        ),
        f"src/{package}/jobs/{name}/job.py": _template(
            f'''\
            """Pure workflow steps for {name}."""

            from {package}.runtime import JobConfigurationError

            Metadata = dict[str, int | str]


            def extract() -> Metadata:
                raise JobConfigurationError("Implement {name}.extract")


            def transform(metadata: Metadata) -> Metadata:
                raise JobConfigurationError("Implement {name}.transform")


            def load(metadata: Metadata) -> None:
                raise JobConfigurationError("Implement {name}.load")
            '''
        ),
        f"tests/unit/jobs/test_{name}.py": _template(
            f"""\
            import pytest

            from {package}.jobs.{name} import extract
            from {package}.runtime import JobConfigurationError


            def test_placeholder_fails_clearly() -> None:
                with pytest.raises(
                    JobConfigurationError,
                    match=r"Implement {name}\\.extract",
                ):
                    extract()
            """
        ),
    }


def _isolated_files(package: str, name: str) -> dict[str, str]:
    return {
        f"dags/{name}.py": _template(
            f'''\
            """Isolated execution scaffold for {name}.

            Replace the Python path with a prepared worker environment or switch to the native
            Docker/Kubernetes/provider operator used by your infrastructure.
            """

            from airflow.sdk import dag, task

            from {package}.runtime import JobSpec, TaskPolicy

            SPEC = JobSpec(
                dag_id="{name}",
                description="Generated isolated job {name}; update before scheduling.",
                schedule=None,
                tags=("generated", "isolated"),
                task_policy=TaskPolicy(retries=0),
            )


            DAG_KWARGS = SPEC.as_dag_kwargs()
            DAG_SCHEDULE = DAG_KWARGS.pop("schedule")


            @dag(schedule=DAG_SCHEDULE, **DAG_KWARGS)
            def workflow():
                @task.external_python(
                    python="/path/to/specialized/venv/bin/python",
                    **SPEC.task_policy.as_task_kwargs(),
                )
                def execute_isolated() -> None:
                    raise RuntimeError("Configure the isolated runtime and implement this task")

                execute_isolated()


            dag = workflow()
            '''
        ),
        f"tests/unit/jobs/test_{name}.py": _template(
            f"""\
            from pathlib import Path


            def test_isolated_dag_keeps_explicit_runtime_placeholder() -> None:
                text = Path("dags/{name}.py").read_text(encoding="utf-8")
                assert "@task.external_python" in text
                assert "/path/to/specialized/venv/bin/python" in text
            """
        ),
    }


def build_files(package: str, name: str, job_type: str) -> dict[str, str]:
    if job_type == "simple":
        return _simple_files(package, name)
    if job_type == "workflow":
        return _workflow_files(package, name)
    if job_type == "isolated":
        return _isolated_files(package, name)
    raise ScaffoldError(f"unsupported job type: {job_type}")


def create_job(root: Path, name: str, job_type: str) -> list[Path]:
    root = root.resolve()
    name = validate_job_name(name)
    package, bootstrapped = _state(root)
    if not bootstrapped:
        raise ScaffoldError("run scripts/bootstrap_project.py before creating jobs")
    source_root = (root / "src").resolve()
    package_dir = (source_root / package).resolve()
    if package_dir.parent != source_root or not package_dir.is_dir():
        raise ScaffoldError("configured package directory must be a direct child of src/")

    planned = build_files(package, name, job_type)
    for relative in planned:
        destination = (root / relative).resolve()
        if not destination.is_relative_to(root):
            raise ScaffoldError(f"generated path escapes repository root: {relative}")
    existing = [root / relative for relative in planned if (root / relative).exists()]
    if existing:
        joined = ", ".join(str(path.relative_to(root)) for path in existing)
        raise ScaffoldError(f"job already exists or paths are occupied: {joined}")

    created: list[Path] = []
    try:
        for relative, content in planned.items():
            path = root / relative
            _atomic_create(path, content)
            created.append(path)
    except Exception:
        for path in created:
            path.unlink(missing_ok=True)
        raise
    return created


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("job_name")
    parser.add_argument("--type", choices=("simple", "workflow", "isolated"), default="simple")
    parser.add_argument("--root", type=Path, default=Path.cwd(), help="repository root")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        created = create_job(args.root, args.job_name, args.type)
    except ScaffoldError as exc:
        print(f"scaffold failed: {exc}", file=sys.stderr)
        return 2
    print(f"created {args.type} job {validate_job_name(args.job_name)!r}:")
    for path in created:
        print(f"- {path.relative_to(args.root.resolve())}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
