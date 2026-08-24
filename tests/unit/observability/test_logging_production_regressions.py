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


def test_non_finite_numbers_do_not_emit_invalid_json(job_context) -> None:
    logger, stream = _logger_with_stream("test.nonfinite")
    log_event(logger, "metric", context=job_context, ratio=float("nan"))
    payload = json.loads(stream.getvalue())
    assert payload["ratio"] == "<non-finite-float>"
    assert "NaN" not in stream.getvalue()


def test_caller_fields_cannot_override_execution_identity(job_context) -> None:
    logger, stream = _logger_with_stream("test.identity")
    log_event(
        logger,
        "event",
        context=job_context,
        dag_id="spoofed",
        task_id="spoofed",
        run_id="spoofed",
        try_number=999,
    )
    payload = json.loads(stream.getvalue())
    assert payload["dag_id"] == job_context.dag_id
    assert payload["task_id"] == job_context.task_id
    assert payload["run_id"] == job_context.run_id
    assert payload["try_number"] == job_context.try_number
