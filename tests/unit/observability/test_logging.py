import json
import logging
from io import StringIO

from airflow_job_template.observability import log_event


def test_log_event_redacts_sensitive_field_names(job_context) -> None:
    stream = StringIO()
    logger = logging.getLogger("test.structured")
    logger.handlers = [logging.StreamHandler(stream)]
    logger.propagate = False
    logger.setLevel(logging.INFO)

    sensitive_value = "do-not-log"
    redacted_marker = "<redacted>"
    log_event(
        logger,
        "job_completed",
        context=job_context,
        processed=3,
        api_token=sensitive_value,
    )
    payload = json.loads(stream.getvalue())

    assert payload["event"] == "job_completed"
    assert payload["processed"] == 3
    assert payload["api_token"] == redacted_marker
    assert payload["dag_id"] == "unit_test_job"
