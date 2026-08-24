"""Filesystem helpers for work that remains inside one task execution"""

from __future__ import annotations

import hashlib
import os
import re
import tempfile
from pathlib import Path

from airflow_job_template.runtime.errors import JobConfigurationError

_SAFE_FILENAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
_WINDOWS_RESERVED_BASENAMES = {
    "CON",
    "PRN",
    "AUX",
    "NUL",
    *(f"COM{index}" for index in range(1, 10)),
    *(f"LPT{index}" for index in range(1, 10)),
}


def safe_filename(value: str, *, max_length: int = 180) -> str:
    """Validate one portable filename component and reject traversal/shell-like input"""

    if isinstance(max_length, bool) or not isinstance(max_length, int) or max_length < 1:
        raise JobConfigurationError("max_length must be an integer >= 1")
    if not isinstance(value, str) or not value or len(value) > max_length:
        raise JobConfigurationError(f"filename must be a string of 1..{max_length} characters")
    if value in {".", ".."} or "/" in value or "\\" in value:
        raise JobConfigurationError("filename must not contain path separators")
    if value.endswith("."):
        raise JobConfigurationError("filename must not end with a dot")
    if not _SAFE_FILENAME_RE.fullmatch(value):
        raise JobConfigurationError(
            "filename may contain only letters, numbers, dot, underscore and hyphen"
        )
    basename = value.split(".", 1)[0].upper()
    if basename in _WINDOWS_RESERVED_BASENAMES:
        raise JobConfigurationError(f"filename {value!r} is reserved on Windows")
    return value


def sha256_file(path: str | Path, *, chunk_size: int = 1024 * 1024) -> str:
    """Return a streaming SHA-256 digest without loading the whole artifact into memory"""

    if isinstance(chunk_size, bool) or not isinstance(chunk_size, int) or chunk_size < 1:
        raise ValueError("chunk_size must be an integer >= 1")
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _validated_mode(mode: int) -> int:
    if isinstance(mode, bool) or not isinstance(mode, int) or not 0 <= mode <= 0o777:
        raise JobConfigurationError("mode must be an integer between 0o000 and 0o777")
    return mode


def atomic_write_bytes(path: str | Path, data: bytes, *, mode: int = 0o600) -> Path:
    """Write a file atomically in its destination directory"""

    if not isinstance(data, bytes):
        raise JobConfigurationError("data must be bytes")
    mode = _validated_mode(mode)
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{destination.name}.", dir=destination.parent)
    temp_path = Path(temp_name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temp_path, mode)
        os.replace(temp_path, destination)
    except Exception:
        temp_path.unlink(missing_ok=True)
        raise
    return destination


def atomic_write_text(
    path: str | Path,
    text: str,
    *,
    encoding: str = "utf-8",
    mode: int = 0o600,
) -> Path:
    """Atomically write text by delegating to the byte-safe implementation"""

    if not isinstance(text, str):
        raise JobConfigurationError("text must be a string")
    if not isinstance(encoding, str) or not encoding.strip():
        raise JobConfigurationError("encoding must be a non-blank string")
    return atomic_write_bytes(path, text.encode(encoding), mode=mode)
