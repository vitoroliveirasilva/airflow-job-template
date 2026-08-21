import json
import logging
from io import StringIO

from airflow_job_template.observability import log_event


def _logger_with_stream(name: str) -> tuple[logging.Logger, StringIO]:
    stream = StringIO()
    logger = logging.getLogger(name)
    logger.handlers = [logging.StreamHandler(stream)]
    logger.propagate = False
    logger.setLevel(logging.INFO)
    return logger, stream


def test_log_event_redacts_sensitive_field_names(job_context) -> None:
    logger, stream = _logger_with_stream("test.structured")

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


def test_log_event_redacts_credentials_embedded_in_uri(job_context) -> None:
    logger, stream = _logger_with_stream("test.uri")
    uri_value = "https://user:" + "password" + "@example.invalid/report.csv"

    log_event(logger, "artifact_ready", context=job_context, artifact_uri=uri_value)
    payload = json.loads(stream.getvalue())

    assert payload["artifact_uri"] == "<redacted>"
    assert "password" not in stream.getvalue()


def test_log_event_does_not_call_arbitrary_object_string_representation(job_context) -> None:
    class SecretBearingObject:
        def __str__(self) -> str:
            return "https://user:password@example.invalid/private"

    logger, stream = _logger_with_stream("test.object")
    log_event(logger, "client_ready", context=job_context, client=SecretBearingObject())
    payload = json.loads(stream.getvalue())

    assert payload["client"] == "<SecretBearingObject>"
    assert "password" not in stream.getvalue()
