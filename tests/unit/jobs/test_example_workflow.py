from airflow_job_template.jobs.example_workflow import extract, load, validate


def test_example_workflow_passes_only_small_metadata() -> None:
    extracted = extract(5)
    validated = validate(extracted)
    loaded = load(validated)
    assert loaded == {"batch_id": "example-5", "processed": 5}
