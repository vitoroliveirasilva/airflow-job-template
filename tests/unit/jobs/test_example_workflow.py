import pytest

from airflow_job_template.jobs.example_workflow import extract, load, validate
from airflow_job_template.runtime import JobConfigurationError


def test_example_workflow_passes_only_small_metadata() -> None:
    extracted = extract(5)
    validated = validate(extracted)
    loaded = load(validated)
    assert loaded == {"batch_id": "example-5", "processed": 5}


def test_load_rejects_tampered_validated_metadata() -> None:
    with pytest.raises(JobConfigurationError, match="invalid validated metadata"):
        load({"validated": True})


@pytest.mark.parametrize("limit", [True, 1.5, "5", 0, 10_001])
def test_extract_rejects_invalid_limit_types_and_bounds(limit) -> None:
    with pytest.raises(JobConfigurationError, match="limit"):
        extract(limit)


@pytest.mark.parametrize(
    "metadata",
    [
        {"batch_id": " ", "record_count": 1},
        {"batch_id": "batch", "record_count": True},
    ],
)
def test_validate_rejects_tampered_metadata(metadata) -> None:
    with pytest.raises(JobConfigurationError, match="invalid extract metadata"):
        validate(metadata)
