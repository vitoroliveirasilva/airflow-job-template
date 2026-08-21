"""Fail on high-confidence secret material or forbidden generated-state paths"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

SKIP_DIRS = {".git", ".venv", "__pycache__", ".pytest_cache", ".ruff_cache"}
ALLOWED_ENV_EXAMPLES = {".env.example"}
PATTERNS = {
    "private key": re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    "GitHub token": re.compile(r"\bgh[oprsu]_[A-Za-z0-9]{30,}\b"),
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
        if any(part in SKIP_DIRS for part in path.parts):
            continue
        if not path.is_file():
            continue
        if _is_forbidden_env_file(path):
            findings.append(f"forbidden env file: {path.relative_to(root)}")
            continue
        try:
            raw = path.read_bytes()
        except OSError:
            raise
        if b"\x00" in raw:
            continue
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            continue
        for label, pattern in PATTERNS.items():
            if pattern.search(text):
                findings.append(f"{label}: {path.relative_to(root)}")
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
