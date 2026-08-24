import sys

import pytest

if sys.platform == "win32":
    pytest.skip(
        "Airflow runtime tests require a POSIX environment; use WSL2/Linux or CI",
        allow_module_level=True,
    )

pytest.importorskip("airflow")

from airflow_job_template.runtime import JobResult, JobSpec, build_single_task_dag


def test_single_task_factory_applies_spec_and_policy() -> None:
    def job(_context):
        return JobResult(processed=1)

    spec = JobSpec(dag_id="factory_test", description="Factory test")
    dag = build_single_task_dag(spec=spec, job_callable=job)

    assert dag.dag_id == "factory_test"
    assert dag.catchup is False
    assert list(dag.task_dict) == ["execute"]
    task = dag.task_dict["execute"]
    assert task.retries == spec.task_policy.retries
    assert task.execution_timeout == spec.task_policy.execution_timeout


def test_single_task_factory_rejects_conflicting_task_id_override() -> None:
    spec = JobSpec(dag_id="factory_override_test", description="Factory test")

    with pytest.raises(ValueError, match="dedicated task_id"):
        build_single_task_dag(
            spec=spec,
            job_callable=lambda _context: None,
            task_overrides={"task_id": "other"},
        )


def test_single_task_factory_validates_public_arguments() -> None:
    spec = JobSpec(dag_id="factory_validation_test", description="Factory test")
    with pytest.raises(TypeError, match="job_callable"):
        build_single_task_dag(spec=spec, job_callable=None)
    with pytest.raises(ValueError, match="non-blank"):
        build_single_task_dag(spec=spec, job_callable=lambda _context: None, task_id=" ")
