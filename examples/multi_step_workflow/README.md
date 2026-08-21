# Explicit multi-step workflow

See `dags/example_workflow.py` and `src/airflow_job_template/jobs/example_workflow/job.py`.

The DAG keeps `extract -> validate -> load` visible in Airflow. Only a small batch identifier and a record count cross task boundaries. A real implementation should put large files or datasets in shared/object storage and pass only their URI/identifier through XCom.

Use this pattern when the steps need independent retry, timeout, or observability. If retrying the whole operation is the correct unit, prefer a Simple Job instead.
