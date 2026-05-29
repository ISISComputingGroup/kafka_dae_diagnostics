import time
from unittest.mock import MagicMock

import pytest
from confluent_kafka import Message
from confluent_kafka.cimpl import KafkaError
from streaming_data_types import serialise_6s4t, serialise_ev44, serialise_pl72, serialise_pu00
from streaming_data_types.run_start_pl72 import DetectorSpectrumMap

from kafka_dae_diagnostics.data import Data, FrameMetaData
from kafka_dae_diagnostics.kafka.handlers import (
    handle_6s4t,
    handle_ev44,
    handle_event_messages,
    handle_pl72,
    handle_pu00,
    handle_run_info_messages,
)

FRAME_METADATA = serialise_pu00(
    source_name="",
    message_id=0,
    timestamp_ns=1_234_000_000_000,
    proton_charge=1.23456,
    period_number=0,
    vetos=0,
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

RUN_START_NO_SPECMAP = serialise_pl72(
    job_id="a job id",
    filename="a file name",
    start_time=1234 * 1_000,
    run_name="a really nice run",
    instrument_name="GREAT_INSTRUMENT",
)

RUN_STOP = serialise_6s4t(job_id="a job id", run_name="a really nice run", stop_time=1234 * 1_000)

INVALID_MSG = b"\0\0\0\0\0\0\0\0"


def make_message(b: bytes | None, partition: int = 0) -> Message:
    ret = MagicMock(spec=Message)
    ret.value.return_value = b
    ret.error.return_value = None
    ret.partition.return_value = partition
    return ret


def error_message() -> Message:
    error = MagicMock(spec=KafkaError)
    error.code.return_value = -123456

    ret = MagicMock(spec=Message)
    ret.error.return_value = error
    ret.value.return_value = None
    ret.partition.return_value = None
    return ret


def test_handle_event_messages():
    msg0 = make_message(FRAME_METADATA)
    msg1 = make_message(ONE_EVENT)
    msg2 = make_message(ONE_EVENT)
    msg3 = error_message()
    msg4 = make_message(None)

    data = Data()
    handle_event_messages([msg0, msg1, msg2, msg3, msg4], data)

    assert data.total_events == 2


def test_handle_ev44():
    data = Data(frame_metadata={1: FrameMetaData(period=0, proton_charge=1.23, vetoes=0)})
    handle_ev44(data, make_message(ONE_EVENT, partition=1))

    assert data.total_events == 1
    assert data.largest_kafka_timestamp == 1234
    assert data.most_recent_kafka_timestamp == 1234
    assert data.total_event_messages == 1
    assert data.event_processing_lag == pytest.approx(time.time() - 1234, abs=2)


def test_handle_ev44_without_metadata():
    data = Data()
    handle_ev44(data, make_message(ONE_EVENT))
    assert data.total_events == 0


def test_handle_ev44_with_invalid_period_number():
    data = Data(frame_metadata={1: FrameMetaData(period=987654321, proton_charge=1.23, vetoes=0)})
    handle_ev44(
        data,
        make_message(
            serialise_ev44(
                source_name="",
                message_id=0,
                reference_time=[1_234_000_000_000],
                reference_time_index=[0],
                pixel_id=[0],
                time_of_flight=[0],
            ),
            partition=1,
        ),
    )

    # Event not counted or histogrammed
    assert data.total_events == 0
    assert data.spectra.sum() == 0


def test_handle_vetoed_ev44():
    data = Data(
        frame_metadata={1: FrameMetaData(period=0, proton_charge=1.23, vetoes=0xFFFF)}, veto_mask=1
    )
    handle_ev44(data, make_message(ONE_EVENT, partition=1))

    assert data.total_event_messages == 1
    assert data.total_events == 0
    assert data.spectra.sum() == 0


def test_handle_pu00_with_invalid_period_number():
    data = Data()
    handle_pu00(
        data,
        make_message(
            serialise_pu00(
                source_name="",
                message_id=0,
                timestamp_ns=1234_000_000_000,
                period_number=987654321,
                proton_charge=1.23,
                vetos=0,
            )
        ),
    )

    # Good/raw frame gets counted normally...
    assert data.raw_frames == 1
    assert data.good_frames == 1
    assert data.raw_uah == pytest.approx(1.23)
    assert data.good_uah == pytest.approx(1.23)

    # But not into any per-period array
    assert data.good_uah_pd.sum() == pytest.approx(0)
    assert data.raw_uah_pd.sum() == pytest.approx(0)


def test_handle_vetoed_pu00():
    msg = MagicMock(spec=Message)
    msg.error.return_value = None
    msg.value.return_value = serialise_pu00(
        source_name="",
        message_id=0,
        timestamp_ns=1234_000_000_000,
        period_number=0,
        proton_charge=1.23,
        vetos=0xFFFF,
    )

    data = Data(veto_mask=1)
    handle_pu00(
        data,
        msg,
    )

    assert data.raw_frames == 1
    assert data.good_frames == 0
    assert data.raw_uah == pytest.approx(1.23)
    assert data.good_uah == pytest.approx(0)

    assert data.raw_uah_pd.sum() == pytest.approx(1.23)
    assert data.good_uah_pd.sum() == pytest.approx(0)


def test_handle_invalid_pu00():
    msg = make_message(b"\0\0\0\0\0\0\0\0")
    handle_pu00(Data(), msg)


def test_handle_runinfo_msg():
    data = Data()

    msg1 = make_message(RUN_START)
    msg2 = make_message(RUN_STOP)
    msg3 = error_message()
    msg4 = make_message(None)

    handle_run_info_messages([msg1, msg2, msg3, msg4], data, MagicMock())

    assert data.start_time == 1234
    assert data.stop_time == 1234


@pytest.mark.parametrize("run_start", [RUN_START, RUN_START_NO_SPECMAP])
def test_handle_pl72(run_start: bytes):
    data = Data(
        total_events=5,
        total_event_messages=5,
        total_event_megabytes=5,
        largest_kafka_timestamp=54321,
        most_recent_kafka_timestamp=54321,
        start_time=54321,
    )
    event_consumer = MagicMock()

    # Check we handle multiple runstarts in a row...
    handle_pl72(data, make_message(run_start), event_consumer)
    handle_pl72(data, make_message(run_start), event_consumer)

    assert data.total_events == 0
    assert data.total_event_messages == 0
    assert data.total_event_megabytes == 0
    assert data.largest_kafka_timestamp == 1234
    assert data.most_recent_kafka_timestamp == 1234
    assert data.start_time == 1234


def test_handle_6s4t():
    data = Data(stop_time=54321)
    handle_6s4t(data, make_message(RUN_STOP))
    assert data.stop_time == 1234


def test_handle_invalid_msg_ignored():
    msg = MagicMock(spec=Message)
    msg.error.return_value = None
    msg.value.return_value = INVALID_MSG
    msg.partition.return_value = 1

    # Should not crash - messages ignored silently if unrecognised
    handle_event_messages([msg], Data())
    handle_run_info_messages([msg], Data(), MagicMock())


def test_handle_invalid_ev44():
    data = Data()
    handle_ev44(data, make_message(b"\1\2\3\4" + b"ev44"))


def test_handle_invalid_6s4t():
    data = Data()
    handle_6s4t(data, make_message(b"\1\2\3\4" + b"pl72"))


def test_handle_invalid_pl72():
    data = Data()
    handle_pl72(data, make_message(b"\1\2\3\4" + b"pl72"), MagicMock())
