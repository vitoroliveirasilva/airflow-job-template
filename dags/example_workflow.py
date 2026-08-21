"""Explicit TaskFlow workflow; topology stays visible in the DAG file"""

from airflow.sdk import Param, dag, get_current_context, task

from airflow_job_template.jobs.example_workflow import extract, load, validate
from airflow_job_template.runtime import JobSpec, TaskPolicy

POLICY = TaskPolicy(retries=2)
SPEC = JobSpec(
    dag_id="example_workflow",
    description="Explicit extract -> validate -> load workflow using small XCom metadata.",
    schedule=None,
    tags=("example", "workflow"),
    params={"limit": Param(100, type="integer", minimum=1, maximum=10_000)},
    task_policy=POLICY,
)


@dag(**SPEC.as_dag_kwargs())
def workflow():
    @task(**POLICY.as_task_kwargs())
    def extract_task() -> dict:
        context = get_current_context()
        return extract(int(context["params"]["limit"]))

    @task(**POLICY.as_task_kwargs())
    def validate_task(metadata: dict) -> dict:
        return validate(metadata)

    @task(**POLICY.as_task_kwargs())
    def load_task(metadata: dict) -> dict:
        return load(metadata)

    load_task(validate_task(extract_task()))


dag = workflow()
