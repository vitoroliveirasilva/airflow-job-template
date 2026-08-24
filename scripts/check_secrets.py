"""Fail on high-confidence secret material or forbidden generated-state paths"""

from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path

SKIP_DIRS = {
    ".git",
    ".venv",
    "__pycache__",
    ".pytest_cache",
    ".ruff_cache",
    ".airflow",
    ".tmp",
    "build",
    "dist",
}
ALLOWED_ENV_EXAMPLES = {".env.example"}
PATTERNS = {
    "private key": re.compile(
        r"-----BEGIN (?:(?:RSA|EC|DSA|OPENSSH) )?PRIVATE KEY-----"
        + r"|-----BEGIN PGP "
        + r"PRIVATE KEY BLOCK-----"
    ),
    "GitHub token": re.compile(r"\b(?:gh[oprsu]_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9_]{20,})\b"),
    "GitLab token": re.compile(r"\bglpat-[A-Za-z0-9_-]{20,}\b"),
    "AWS access key": re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b"),
    "Google API key": re.compile(r"\bAIza[0-9A-Za-z_-]{35}\b"),
    "npm token": re.compile(r"\bnpm_[A-Za-z0-9]{36}\b"),
    "PyPI token": re.compile(r"\bpypi-AgEIcHlwaS5vcmc[A-Za-z0-9_-]{20,}\b"),
    "Slack token": re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{20,}\b"),
    "Stripe live secret": re.compile(r"\bsk_live_[A-Za-z0-9]{16,}\b"),
    "credential URI": re.compile(r"\b[A-Za-z][A-Za-z0-9+.-]*://[^/\s:@]+:[^/\s@]+@"),
}


def _is_forbidden_env_file(path: Path) -> bool:
    return path.name.startswith(".env") and path.name not in ALLOWED_ENV_EXAMPLES


def _is_link_like(path: Path) -> bool:
    """Detect symlinks and Windows junctions without following them"""

    is_junction = getattr(path, "is_junction", None)
    return path.is_symlink() or (is_junction is not None and is_junction())


def _raise_walk_error(error: OSError) -> None:
    raise error


def _scan_files(root: Path) -> list[Path]:
    paths: list[Path] = []
    for current_dir, dirnames, filenames in os.walk(
        root,
        topdown=True,
        onerror=_raise_walk_error,
        followlinks=False,
    ):
        current = Path(current_dir)
        dirnames[:] = [
            name for name in dirnames if name not in SKIP_DIRS and not _is_link_like(current / name)
        ]
        paths.extend(current / name for name in filenames)
    return sorted(paths, key=lambda item: item.relative_to(root).as_posix())


def scan(root: Path) -> list[str]:
    root = root.resolve()
    if not root.is_dir():
        raise ValueError(f"scan root is not a directory: {root}")
    findings: list[str] = []
    for path in _scan_files(root):
        relative = path.relative_to(root)
        if _is_forbidden_env_file(path) and (_is_link_like(path) or path.is_file()):
            findings.append(f"forbidden env file: {relative}")
            continue
        if _is_link_like(path) or not path.is_file():
            continue
        raw = path.read_bytes()
        if b"\x00" in raw:
            continue
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            continue
        for label, pattern in PATTERNS.items():
            if pattern.search(text):
                findings.append(f"{label}: {relative}")
    return findings


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", nargs="?", type=Path, default=Path.cwd())
    args = parser.parse_args(argv)
    try:
        findings = scan(args.root)
    except (OSError, ValueError) as exc:
        print(f"secret check failed: {exc}", file=sys.stderr)
        return 2
    if findings:
        print("secret check failed:", file=sys.stderr)
        for finding in findings:
            print(f"- {finding}", file=sys.stderr)
        return 1
    print("secret check passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
