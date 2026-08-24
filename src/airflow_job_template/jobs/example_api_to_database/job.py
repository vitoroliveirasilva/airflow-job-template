"""Example API -> PostgreSQL job using runtime Connections and idempotent upsert"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

from airflow_job_template.integrations import DatabaseClient, HttpClient
from airflow_job_template.observability import log_event
from airflow_job_template.runtime import JobConfigurationError, JobResult, JobRunContext

CRM_API_CONN_ID = "example_crm_api"
TARGET_DB_CONN_ID = "example_analytics_postgres"

_UPSERT_SQL = """
INSERT INTO customer_sync (external_id, name, updated_at)
VALUES (%s, %s, %s)
ON CONFLICT (external_id)
DO UPDATE SET name = EXCLUDED.name, updated_at = EXCLUDED.updated_at
""".strip()


def _to_row(item: Any) -> tuple[str, str, str]:
    if not isinstance(item, Mapping):
        raise JobConfigurationError("API customer item must be an object")
    external_id = item.get("id")
    name = item.get("name")
    updated_at = item.get("updated_at")
    values = (external_id, name, updated_at)
    if not all(isinstance(value, str) and value.strip() for value in values):
        raise JobConfigurationError("API customer item is missing id, name or updated_at")
    return (external_id, name, updated_at)


def run(
    context: JobRunContext,
    *,
    http_client: HttpClient | None = None,
    database_client: DatabaseClient | None = None,
) -> JobResult:
    """Fetch pages and upsert batches; no network or DB work occurs during DAG parsing"""

    batch_size = context.params.get("batch_size", 500)
    dry_run = context.params.get("dry_run", True)
    if isinstance(batch_size, bool) or not isinstance(batch_size, int):
        raise JobConfigurationError("batch_size must be an integer")
    if not 1 <= batch_size <= 5_000:
        raise JobConfigurationError("batch_size must be between 1 and 5000")
    if not isinstance(dry_run, bool):
        raise JobConfigurationError("dry_run must be a boolean")

    http = http_client or HttpClient(CRM_API_CONN_ID)
    database = database_client or DatabaseClient(TARGET_DB_CONN_ID)
    logger = logging.getLogger(__name__)

    processed = 0
    pending: list[tuple[str, str, str]] = []
    log_event(logger, "source_read_started", context=context, external_system="crm_api")

    for item in http.iter_offset_items(
        "/v1/customers",
        item_key="items",
        page_size=batch_size,
        max_pages=1_000,
    ):
        pending.append(_to_row(item))
        if len(pending) >= batch_size:
            if not dry_run:
                database.executemany(_UPSERT_SQL, pending, chunk_size=batch_size)
            processed += len(pending)
            pending.clear()

    if pending:
        if not dry_run:
            database.executemany(_UPSERT_SQL, pending, chunk_size=batch_size)
        processed += len(pending)

    log_event(logger, "write_completed", context=context, processed=processed, dry_run=dry_run)
    return JobResult(processed=processed)
