import pytest
from examples.sap_query.example import run

from airflow_job_template.runtime import NonRetryableJobError


class Reader:
    def query(self, entity_set, *, params):
        assert entity_set == "OpenOrders"
        assert params == {"$select": "OrderId,Status"}
        return [{"OrderId": "1"}, {"OrderId": "2"}]


def test_sap_example_is_protocol_injected_and_returns_metadata(job_context) -> None:
    result = run(job_context, Reader())
    assert result.processed == 2
    assert result.batch_id == job_context.run_id


def test_sap_example_rejects_invalid_reader_contract(job_context) -> None:
    class InvalidReader:
        def query(self, entity_set, *, params):
            return "not a row sequence"

    with pytest.raises(NonRetryableJobError, match="non-sequence"):
        run(job_context, InvalidReader())
