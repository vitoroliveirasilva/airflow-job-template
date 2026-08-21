from examples.sap_query.example import run


class Reader:
    def query(self, entity_set, *, params):
        assert entity_set == "OpenOrders"
        assert params == {"$select": "OrderId,Status"}
        return [{"OrderId": "1"}, {"OrderId": "2"}]


def test_sap_example_is_protocol_injected_and_returns_metadata(job_context) -> None:
    result = run(job_context, Reader())
    assert result.processed == 2
    assert result.batch_id == job_context.run_id
