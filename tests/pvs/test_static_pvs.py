from unittest import mock
from unittest.mock import MagicMock

import pytest

from kafka_dae_diagnostics.data import Data
from kafka_dae_diagnostics.pvs.static_pvs import StaticPVs, static_pv_provider


@pytest.fixture
def data():
    return Data(
        total_events=5_000_000,
        total_event_messages=123,
        total_event_megabytes=321,
    )


def test_static_pv_provider(data: Data):
    static_pv_provider(prefix="UNITTEST:", data=data)
    assert "static-callbacks" in data.callbacks


def test_static_pvs(data: Data):
    pvs = StaticPVs(data)
    pvs.total_events = MagicMock()
    pvs.total_mevents = MagicMock()
    pvs.total_event_megabytes = MagicMock()
    pvs.update_all(data=data)
    pvs.total_events.post.assert_called_once_with(5_000_000, timestamp=mock.ANY)
    pvs.total_mevents.post.assert_called_once_with(5, timestamp=mock.ANY)
    pvs.total_event_megabytes.post.assert_called_once_with(321, timestamp=mock.ANY)
