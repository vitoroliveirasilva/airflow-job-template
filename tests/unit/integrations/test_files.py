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
