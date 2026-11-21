import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "examples"))
import mock_server  # noqa: E402


@pytest.fixture(scope="session")
def server():
    srv, base = mock_server.start(0)
    yield base
    srv.shutdown()


@pytest.fixture(autouse=True)
def _reset_mock_state():
    mock_server.STATE["flaky_calls"] = 0
    mock_server.STATE["slack"].clear()


@pytest.fixture
def slack_posts():
    return mock_server.STATE["slack"]


@pytest.fixture
def paths(tmp_path):
    return {
        "state": tmp_path / "state.db",
        "db": f"sqlite:///{tmp_path / 'wh.db'}",
        "lake": str(tmp_path / "lake"),
        "rejects": str(tmp_path / "rejects"),
        "tmp": tmp_path,
    }
