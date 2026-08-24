from pathlib import Path

import pytest

from airflow_job_template.integrations.files import (
    atomic_write_text,
    safe_filename,
    sha256_file,
)
from airflow_job_template.runtime import JobConfigurationError


def test_safe_filename_rejects_traversal() -> None:
    with pytest.raises(JobConfigurationError):
        safe_filename("../secret.txt")


def test_atomic_write_and_checksum(tmp_path: Path) -> None:
    path = atomic_write_text(tmp_path / "report.txt", "hello")
    assert path.read_text(encoding="utf-8") == "hello"
    assert sha256_file(path) == "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"


def test_safe_filename_rejects_invalid_runtime_types() -> None:
    with pytest.raises(JobConfigurationError):
        safe_filename(123)
    with pytest.raises(JobConfigurationError, match="max_length"):
        safe_filename("report.csv", max_length=True)


def test_sha256_rejects_boolean_chunk_size(tmp_path: Path) -> None:
    path = tmp_path / "payload.bin"
    path.write_bytes(b"abc")
    with pytest.raises(ValueError, match="integer"):
        sha256_file(path, chunk_size=True)


def test_atomic_text_write_classifies_invalid_encoding_as_configuration_error(
    tmp_path: Path,
) -> None:
    with pytest.raises(JobConfigurationError, match="unknown text encoding"):
        atomic_write_text(tmp_path / "payload.txt", "text", encoding="not-an-encoding")
    with pytest.raises(JobConfigurationError, match="cannot be encoded"):
        atomic_write_text(tmp_path / "payload.txt", "não ASCII", encoding="ascii")
    assert not (tmp_path / "payload.txt").exists()
