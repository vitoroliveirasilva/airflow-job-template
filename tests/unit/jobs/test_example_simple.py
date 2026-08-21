from airflow_job_template.jobs.example_simple import run


def test_example_simple_returns_small_result(job_context) -> None:
    result = run(job_context)
    assert result.to_xcom()["processed"] == 1
