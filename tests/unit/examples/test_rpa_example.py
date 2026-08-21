from pathlib import Path

import pytest
from examples.rpa.example import update_record


class Session:
    def __init__(self, state="pending", *, valid=True):
        self.state = state
        self.valid = valid
        self.applied = False
        self.closed = False
        self.screenshot_path = None

    def current_state(self, record_id):
        return self.state

    def apply_update(self, record_id):
        self.applied = True

    def validate_update(self, record_id):
        return self.valid

    def screenshot(self, path):
        self.screenshot_path = path

    def close(self):
        self.closed = True


def test_rpa_skips_already_applied_state_and_cleans_up(job_context, tmp_path: Path) -> None:
    session = Session(state="updated")
    result = update_record(
        job_context,
        record_id="42",
        session=session,
        diagnostic_path=tmp_path / "failure.png",
    )
    assert result.skipped == 1
    assert session.applied is False
    assert session.closed is True


def test_rpa_captures_diagnostic_on_failed_validation(job_context, tmp_path: Path) -> None:
    session = Session(valid=False)
    diagnostic = tmp_path / "failure.png"
    with pytest.raises(RuntimeError, match="did not confirm"):
        update_record(
            job_context,
            record_id="42",
            session=session,
            diagnostic_path=diagnostic,
        )
    assert session.screenshot_path == diagnostic
    assert session.closed is True
