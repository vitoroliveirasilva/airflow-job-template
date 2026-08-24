"""Fail on high-confidence secret material or forbidden generated-state paths"""

from __future__ import annotations

import argparse
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
    "AWS access key": re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b"),
    "Slack token": re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{20,}\b"),
    "credential URI": re.compile(r"\b[A-Za-z][A-Za-z0-9+.-]*://[^/\s:@]+:[^/\s@]+@"),
}


def _is_forbidden_env_file(path: Path) -> bool:
    return path.name.startswith(".env") and path.name not in ALLOWED_ENV_EXAMPLES


def scan(root: Path) -> list[str]:
    root = root.resolve()
    if not root.is_dir():
        raise ValueError(f"scan root is not a directory: {root}")
    findings: list[str] = []
    for path in sorted(root.rglob("*"), key=lambda item: item.as_posix()):
        relative = path.relative_to(root)
        if any(part in SKIP_DIRS for part in relative.parts):
            continue
        if _is_forbidden_env_file(path) and (path.is_file() or path.is_symlink()):
            findings.append(f"forbidden env file: {relative}")
            continue
        if path.is_symlink() or not path.is_file():
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
