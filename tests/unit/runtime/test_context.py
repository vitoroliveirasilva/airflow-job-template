from types import SimpleNamespace

import pytest

from airflow_job_template.runtime import (
    JobConfigurationError,
    JobResult,
    job_run_context_from_airflow,
)


def test_context_adapter_reads_task_instance_without_airflow_models() -> None:
    ti = SimpleNamespace(dag_id="sync", task_id="extract", run_id="run-1", try_number=2)
    context = job_run_context_from_airflow({"ti": ti, "params": {"limit": 10}})

    assert context.dag_id == "sync"
    assert context.task_id == "extract"
    assert context.run_id == "run-1"
    assert context.try_number == 2
    assert context.params["limit"] == 10


def test_context_params_are_immutable_copy() -> None:
    source = {"limit": 10}
    ti = SimpleNamespace(dag_id="sync", task_id="extract", run_id="run-1", try_number=1)
    context = job_run_context_from_airflow({"ti": ti, "params": source})
    source["limit"] = 99
    assert context.params["limit"] == 10


def test_context_adapter_fails_clearly_when_ids_are_missing() -> None:
    with pytest.raises(JobConfigurationError, match="missing"):
        job_run_context_from_airflow({"params": {}})


def test_job_result_only_emits_small_populated_metadata() -> None:
    result = JobResult(processed=3, artifact_uri="s3://bucket/key", batch_id="batch-1")
    assert result.to_xcom() == {
        "processed": 3,
        "artifact_uri": "s3://bucket/key",
        "batch_id": "batch-1",
    }


@pytest.mark.parametrize(
    "artifact_uri",
    [
        "https://user:password@example.invalid/report.csv",
        "https://example.invalid/report.csv?access_token=secret-value",
        "https://example.invalid/report.csv?X-Amz-Signature=secret-value",
    ],
)
def test_job_result_rejects_secret_bearing_artifact_uri(artifact_uri: str) -> None:
    with pytest.raises(JobConfigurationError, match="must not contain credentials or tokens"):
        JobResult(artifact_uri=artifact_uri)
