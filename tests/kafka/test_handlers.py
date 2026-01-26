import time
from unittest.mock import MagicMock

import pytest
from confluent_kafka import Message
from streaming_data_types import serialise_6s4t, serialise_ev44, serialise_pl72
from streaming_data_types.run_start_pl72 import DetectorSpectrumMap

from kafka_dae_diagnostics.data import Data
from kafka_dae_diagnostics.kafka.handlers import (
    handle_6s4t,
    handle_ev44,
    handle_event_messages,
    handle_pl72,
    handle_run_info_messages,
)

ONE_EVENT = serialise_ev44(
    source_name="",
    message_id=0,
    reference_time=[1_234_000_000_000],
    reference_time_index=[0],
    pixel_id=[0],
    time_of_flight=[0],
)

RUN_START = serialise_pl72(
    job_id="a job id",
    filename="a file name",
    start_time=1234 * 1_000,
    run_name="a really nice run",
    instrument_name="GREAT_INSTRUMENT",
    detector_spectrum_map=DetectorSpectrumMap(detector_ids=[0], spectrum_numbers=[0], n_spectra=1),
)

RUN_STOP = serialise_6s4t(job_id="a job id", run_name="a really nice run", stop_time=1234 * 1_000)

INVALID_MSG = b"\0\0\0\0\0\0\0\0"


def test_handle_event_messages():
    msg1 = MagicMock(spec=Message)
    msg1.value.return_value = ONE_EVENT

    msg2 = MagicMock(spec=Message)
    msg2.value.return_value = ONE_EVENT

    msg3 = MagicMock(spec=Message)
    msg3.value.return_value = None

    msg4 = MagicMock(spec=Message)
    msg4.value.return_value = None
    msg4.error.return_value = None

    data = Data()
    handle_event_messages([msg1, msg2, msg3, msg4], data)

    assert data.total_events == 2


def test_handle_ev44():
    data = Data()
    handle_ev44(data, ONE_EVENT)

    assert data.total_events == 1
    assert data.largest_kafka_timestamp == 1234
    assert data.most_recent_kafka_timestamp == 1234
    assert data.total_event_messages == 1
    assert data.event_processing_lag == pytest.approx(time.time() - 1234, abs=2)


def test_handle_runinfo_msg():
    data = Data()

    msg1 = MagicMock(spec=Message)
    msg1.value.return_value = RUN_START

    msg2 = MagicMock(spec=Message)
    msg2.value.return_value = RUN_STOP

    msg3 = MagicMock(spec=Message)
    msg3.value.return_value = None

    msg4 = MagicMock(spec=Message)
    msg4.value.return_value = None
    msg4.error.return_value = None

    handle_run_info_messages([msg1, msg2, msg3, msg4], data, MagicMock())

    assert data.start_time == 1234
    assert data.stop_time == 1234


def test_handle_pl72():
    data = Data(
        total_events=5,
        total_event_messages=5,
        total_event_megabytes=5,
        largest_kafka_timestamp=54321,
        most_recent_kafka_timestamp=54321,
        start_time=54321,
    )
    event_consumer = MagicMock()

    handle_pl72(data, RUN_START, event_consumer)

    assert data.total_events == 0
    assert data.total_event_messages == 0
    assert data.total_event_megabytes == 0
    assert data.largest_kafka_timestamp == 1234
    assert data.most_recent_kafka_timestamp == 1234
    assert data.start_time == 1234


def test_handle_6s4t():
    data = Data(stop_time=54321)
    handle_6s4t(data, RUN_STOP)
    assert data.stop_time == 1234


def test_handle_invalid_msg_ignored():
    msg = MagicMock(spec=Message)
    msg.value.return_value = INVALID_MSG

    # Should not crash - messages ignored silently if unrecognised
    handle_event_messages([msg], Data())
    handle_run_info_messages([msg], Data(), MagicMock())
