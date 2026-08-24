"""Airflow-specific adapters kept outside the pure job/runtime contracts"""

from __future__ import annotations

from collections.abc import Callable

from airflow.sdk.exceptions import AirflowFailException

from .errors import NonRetryableJobError


def run_with_airflow_error_policy[**P, R](
    callable_: Callable[P, R],
    /,
    *args: P.args,
    **kwargs: P.kwargs,
) -> R:
    """
    Run Python job logic and translate permanent job failures to Airflow semantics.

    ``RetryableJobError`` and unexpected exceptions intentionally pass through so Airflow's
    configured retry policy remains in control. ``NonRetryableJobError`` (including
    ``JobConfigurationError``) becomes ``AirflowFailException``, which fails without retry.
    """
    if not callable(callable_):
        raise TypeError("callable_ must be callable")

    try:
        return callable_(*args, **kwargs)
    except NonRetryableJobError as exc:
        message = str(exc).strip() or type(exc).__name__
        raise AirflowFailException(message) from exc
