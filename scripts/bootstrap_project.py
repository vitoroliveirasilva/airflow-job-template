"""Rename the placeholder Python package for a concrete Airflow project"""

from __future__ import annotations

import argparse
import os
import re
import sys
import tempfile
import tomllib
from pathlib import Path

PLACEHOLDER_PACKAGE = "airflow_job_template"
TOOL_NAME = "airflow-job-template"
SKIP_DIRS = {".git", ".venv", "__pycache__", ".pytest_cache", ".ruff_cache", "dist", "build"}
TEXT_SUFFIXES = {".py", ".toml", ".md", ".txt", ".yml", ".yaml"}
PROTECTED_PATHS = {
    Path("scripts/bootstrap_project.py"),
    Path("tests/unit/scripts/test_bootstrap_project.py"),
    Path("tests/unit/scripts/test_new_job.py"),
}
SLUG_RE = re.compile(r"^[a-z][a-z0-9_-]{1,63}$")


class BootstrapError(RuntimeError):
    """Raised when the template cannot be bootstrapped safely"""


def validate_slug(slug: str) -> str:
    normalized = slug.strip().lower()
    if not SLUG_RE.fullmatch(normalized):
        raise BootstrapError(
            "project slug must be 2..64 lowercase characters using letters, numbers, _ or -"
        )
    return normalized


def package_name_for(slug: str) -> str:
    return f"{slug.replace('-', '_')}_airflow"


def _atomic_write(path: Path, content: str) -> None:
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


def _load_state(root: Path) -> dict:
    pyproject = root / "pyproject.toml"
    if not pyproject.is_file():
        raise BootstrapError("pyproject.toml not found; run from the repository root")
    with pyproject.open("rb") as handle:
        data = tomllib.load(handle)
    try:
        return data["tool"][TOOL_NAME]
    except (KeyError, TypeError) as exc:
        raise BootstrapError(f"missing [tool.{TOOL_NAME}] configuration") from exc


def _iter_text_files(root: Path):
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        if any(part in SKIP_DIRS for part in path.parts):
            continue
        if path.name.startswith(".env"):
            continue
        relative = path.relative_to(root)
        if relative in PROTECTED_PATHS:
            continue
        if path.suffix in TEXT_SUFFIXES:
            yield path


def bootstrap(root: Path, slug: str) -> list[Path]:
    root = root.resolve()
    slug = validate_slug(slug)
    package_name = package_name_for(slug)
    state = _load_state(root)

    current_package = state.get("package")
    if state.get("bootstrapped") is True or current_package != PLACEHOLDER_PACKAGE:
        raise BootstrapError("template is already bootstrapped; refusing a second rename")

    source_dir = root / "src" / PLACEHOLDER_PACKAGE
    target_dir = root / "src" / package_name
    if not source_dir.is_dir():
        raise BootstrapError(f"placeholder package not found: {source_dir.relative_to(root)}")
    if target_dir.exists():
        raise BootstrapError(f"target package already exists: {target_dir.relative_to(root)}")

    originals: dict[Path, str] = {}
    replacements: dict[Path, str] = {}
    for path in _iter_text_files(root):
        original = path.read_text(encoding="utf-8")
        updated = original.replace(PLACEHOLDER_PACKAGE, package_name)
        if path == root / "pyproject.toml":
            dist_name = slug.replace("_", "-")
            updated = updated.replace('name = "airflow-job-template"', f'name = "{dist_name}"', 1)
            updated = updated.replace(
                'project_slug = "airflow-job-template"', f'project_slug = "{dist_name}"', 1
            )
            updated = updated.replace("bootstrapped = false", "bootstrapped = true", 1)
        if updated != original:
            originals[path] = original
            replacements[path] = updated

    changed: list[Path] = []
    renamed = False
    try:
        source_dir.rename(target_dir)
        renamed = True
        for path, updated in replacements.items():
            actual_path = path
            if source_dir in path.parents:
                actual_path = target_dir / path.relative_to(source_dir)
            _atomic_write(actual_path, updated)
            changed.append(actual_path)
    except Exception:
        for path, original in originals.items():
            actual_path = path
            if renamed and source_dir in path.parents:
                actual_path = target_dir / path.relative_to(source_dir)
            if actual_path.exists():
                _atomic_write(actual_path, original)
        if renamed and target_dir.exists() and not source_dir.exists():
            target_dir.rename(source_dir)
        raise

    changed.append(target_dir)
    return sorted(set(changed), key=lambda item: str(item))


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("project_slug", help="e.g. customer_sync")
    parser.add_argument("--root", type=Path, default=Path.cwd(), help="repository root")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        changed = bootstrap(args.root, args.project_slug)
    except BootstrapError as exc:
        print(f"bootstrap failed: {exc}", file=sys.stderr)
        return 2
    print("bootstrap complete; changed:")
    for path in changed:
        print(f"- {path.relative_to(args.root.resolve())}")
    print(f"python package: {package_name_for(validate_slug(args.project_slug))}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
