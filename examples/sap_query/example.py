"""
Protocol-specific SAP extension example with no SAP SDK dependency

This file is intentionally not a universal ``SAPClient``. Replace ``ODataReader`` with a concrete adapter only when the deployment's SAP protocol is known and its dependency can live in the chosen worker/runtime.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Protocol

from airflow_job_template.runtime import JobResult, JobRunContext


class ODataReader(Protocol):
    # Small contract a real SAP OData adapter could implement

    def query(
        self, entity_set: str, *, params: Mapping[str, str]
    ) -> Sequence[Mapping[str, Any]]: ...


def run(context: JobRunContext, reader: ODataReader) -> JobResult:
    # Keep protocol access at runtime and return only small execution metadata

    rows = reader.query("OpenOrders", params={"$select": "OrderId,Status"})
    return JobResult(processed=len(rows), batch_id=context.run_id)
