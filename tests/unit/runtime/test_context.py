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
        "https://user:" + "pass" + "word@example.invalid/report.csv",
        "https://example.invalid/report.csv?access_token=secret-value",
        "https://example.invalid/report.csv?X-Amz-Signature=secret-value",
    ],
)
def test_job_result_rejects_secret_bearing_artifact_uri(artifact_uri: str) -> None:
    with pytest.raises(JobConfigurationError, match="must not contain credentials or tokens"):
        JobResult(artifact_uri=artifact_uri)


def test_job_result_rejects_azure_style_sas_signature() -> None:
    sensitive_value = "sas-secret"
    artifact_uri = "https://example.invalid/report.csv?sv=1&sig=" + sensitive_value
    with pytest.raises(JobConfigurationError, match="must not contain credentials or tokens"):
        JobResult(artifact_uri=artifact_uri)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("dag_id", " "),
        ("task_id", None),
        ("run_id", ""),
        ("try_number", True),
        ("try_number", "1"),
        ("params", []),
    ],
)
def test_job_run_context_rejects_invalid_runtime_types(job_context, field: str, value) -> None:
    from dataclasses import replace

    with pytest.raises(JobConfigurationError):
        replace(job_context, **{field: value})


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("processed", True),
        ("created", 1.5),
        ("updated", "3"),
        ("skipped", -1),
        ("artifact_uri", 123),
        ("batch_id", " "),
    ],
)
def test_job_result_rejects_invalid_runtime_types(field: str, value) -> None:
    with pytest.raises(JobConfigurationError):
        JobResult(**{field: value})


def test_job_result_rejects_token_in_uri_fragment() -> None:
    sensitive_value = "fragment-secret"
    artifact_uri = "https://example.invalid/report#access_token=" + sensitive_value
    with pytest.raises(JobConfigurationError, match="must not contain credentials or tokens"):
        JobResult(artifact_uri=artifact_uri)


@pytest.mark.parametrize("try_number", [0, -1, True, 1.5, "1"])
def test_context_adapter_rejects_invalid_try_number(try_number) -> None:
    ti = SimpleNamespace(
        dag_id="sync",
        task_id="extract",
        run_id="run-1",
        try_number=try_number,
    )
    with pytest.raises(JobConfigurationError, match="try_number"):
        job_run_context_from_airflow({"ti": ti, "params": {}})


def test_context_adapter_does_not_coerce_falsy_invalid_params() -> None:
    ti = SimpleNamespace(dag_id="sync", task_id="extract", run_id="run-1", try_number=1)
    with pytest.raises(JobConfigurationError, match="params"):
        job_run_context_from_airflow({"ti": ti, "params": []})


@pytest.mark.parametrize(
    ("field", "value"),
    [("dag_id", " sync"), ("task_id", "extract "), ("run_id", " run-1 ")],
)
def test_job_run_context_rejects_ambiguous_whitespace_in_ids(
    job_context, field: str, value: str
) -> None:
    from dataclasses import replace

    with pytest.raises(JobConfigurationError, match="surrounding whitespace"):
        replace(job_context, **{field: value})


def test_job_run_context_rejects_ambiguous_param_names(job_context) -> None:
    from dataclasses import replace

    with pytest.raises(JobConfigurationError, match="Param names"):
        replace(job_context, params={" limit ": 10})


def test_job_result_rejects_sensitive_semicolon_query_parameters() -> None:
    uri = "https://example.invalid/report.csv?download=1;access_token=secret-value"
    with pytest.raises(JobConfigurationError, match="must not contain credentials or tokens"):
        JobResult(artifact_uri=uri)
