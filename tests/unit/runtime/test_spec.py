from datetime import UTC, datetime

import pytest

from airflow_job_template.runtime import JobConfigurationError, JobSpec


def test_job_spec_has_safe_defaults() -> None:
    spec = JobSpec(dag_id="customer_sync", description="Sync customers")
    kwargs = spec.as_dag_kwargs()

    assert kwargs["schedule"] is None
    assert kwargs["catchup"] is False
    assert kwargs["max_active_runs"] == 1
    assert kwargs["start_date"].tzinfo is not None
    assert kwargs["default_args"]["owner"] == "airflow"


@pytest.mark.parametrize("dag_id", ["CustomerSync", "1_job", "customer-sync", "a" * 101])
def test_job_spec_rejects_unsafe_dag_ids(dag_id: str) -> None:
    with pytest.raises(JobConfigurationError, match="dag_id"):
        JobSpec(dag_id=dag_id, description="x")


def test_job_spec_rejects_dynamic_naive_start_date() -> None:
    with pytest.raises(JobConfigurationError, match="timezone-aware"):
        JobSpec(
            dag_id="customer_sync",
            description="x",
            start_date=datetime(2026, 8, 21),
        )


def test_job_spec_accepts_valid_params_mapping() -> None:
    spec = JobSpec(
        dag_id="manual_job",
        description="manual",
        params={"dry_run": False},
        start_date=datetime(2024, 1, 1, tzinfo=UTC),
    )
    assert spec.as_dag_kwargs()["params"] == {"dry_run": False}


def test_job_spec_copies_mutable_params_and_tags() -> None:
    params = {"limit": 10}
    tags = ["sync"]
    spec = JobSpec(dag_id="immutable_spec", description="immutable", params=params, tags=tags)

    params["limit"] = 99
    tags.append("mutated")

    assert spec.as_dag_kwargs()["params"] == {"limit": 10}
    assert spec.as_dag_kwargs()["tags"] == ["sync"]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("description", None),
        ("schedule", 123),
        ("start_date", "2024-01-01"),
        ("owner", None),
        ("catchup", 1),
        ("max_active_runs", True),
        ("params", []),
        ("tags", "sync"),
    ],
)
def test_job_spec_rejects_wrong_runtime_types(field: str, value) -> None:
    kwargs = {"dag_id": "typed_spec", "description": "x", field: value}
    with pytest.raises(JobConfigurationError):
        JobSpec(**kwargs)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"owner": " airflow "},
        {"params": {" limit ": 10}},
        {"tags": ("sync", "sync")},
    ],
)
def test_job_spec_rejects_ambiguous_or_duplicate_metadata(kwargs: dict) -> None:
    with pytest.raises(JobConfigurationError):
        JobSpec(dag_id="metadata_validation", description="x", **kwargs)
