"""
API -> PostgreSQL example
Connections are resolved only when the task executes
"""

from airflow.sdk import Param

from airflow_job_template.jobs.example_api_to_database import run
from airflow_job_template.runtime import JobSpec, TaskPolicy, build_single_task_dag

SPEC = JobSpec(
    dag_id="example_api_to_database",
    description="Paginated HTTP read followed by batched, idempotent PostgreSQL upsert.",
    schedule=None,
    tags=("example", "http", "database"),
    params={
        "batch_size": Param(500, type="integer", minimum=1, maximum=5_000),
        "dry_run": Param(True, type="boolean"),
    },
    task_policy=TaskPolicy(retries=3),
    doc_md="""
### API to database example

Connections: `example_crm_api` and `example_analytics_postgres`.
The load is idempotent through `ON CONFLICT (external_id) DO UPDATE`.
Client HTTP retries are disabled; Airflow owns the unit-of-work retry.
""",
)

dag = build_single_task_dag(spec=SPEC, job_callable=run)
