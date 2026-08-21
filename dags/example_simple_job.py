"""Minimal single-task job: edit the spec and the job's ``run`` function"""

from airflow_job_template.jobs.example_simple import run
from airflow_job_template.runtime import JobSpec, build_single_task_dag

SPEC = JobSpec(
    dag_id="example_simple_job",
    description="Minimal one-task Python job using the template runtime.",
    schedule=None,
    tags=("example", "simple"),
    doc_md="""
### Example simple job

No external systems. Whole-task retry is safe because the operation is deterministic.
""",
)

dag = build_single_task_dag(spec=SPEC, job_callable=run)
