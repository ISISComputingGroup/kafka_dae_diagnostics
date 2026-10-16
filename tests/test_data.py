from unittest.mock import patch

import numpy as np
import pytest

from kafka_dae_diagnostics.data import Data, RunState


def test_mev():
    data = Data(total_events=5_600_000)
    assert data.mev == pytest.approx(5.6)


def test_duration():
    data = Data(
        largest_kafka_timestamp=20,
        start_time=5,
    )
    assert data.duration == pytest.approx(15)


def test_negative_duration():
    data = Data(
        largest_kafka_timestamp=20,
        start_time=25,
    )
    assert data.duration == pytest.approx(0)


def test_mev_per_hour():
    data = Data(
        largest_kafka_timestamp=3600,
        start_time=0,
        total_events=5_600_000,
    )
    assert data.mev_per_hour == pytest.approx(5.6)

    data.start_time = 3600
    assert data.mev_per_hour == pytest.approx(0.0)


def test_num_periods():
    data = Data(spectra=np.zeros((5, 6, 7)))
    assert data.num_periods == 5


def test_num_spectra():
    data = Data(spectra=np.zeros((5, 6, 7)))
    assert data.num_spectra == 6


def test_num_time_channels():
    data = Data(spectra=np.zeros((5, 6, 7)))
    assert data.num_time_channels == 7


def test_histogram_megabytes():
    data = Data(spectra=np.zeros((10, 11, 12), dtype=np.float64))
    assert data.histogram_megabytes == pytest.approx((10 * 11 * 12 * 8) / (1024**2))


def test_average_data_rate():
    data = Data(
        total_event_megabytes=123.456,
        start_time=0,
        largest_kafka_timestamp=2,
    )
    assert data.average_data_rate == pytest.approx(123.456 / 2)

    data.largest_kafka_timestamp = 0
    assert data.average_data_rate == pytest.approx(0)


def test_count_rate():
    data = Data(
        total_events=123456789,
        start_time=123,
        largest_kafka_timestamp=456,
    )

    assert data.count_rate == pytest.approx(1334.667989)


def test_run_state_setup():
    data = Data(
        start_time=0,
        stop_time=1,
    )
    assert data.run_state == RunState.SETUP


@patch("kafka_dae_diagnostics.data.Data.seconds_since_last_event_message", 1000)
def test_run_state_processing():
    data = Data(
        start_time=2,
        stop_time=1,
    )

    assert data.run_state == RunState.PROCESSING


@patch(
    "kafka_dae_diagnostics.veto_diagnostics.VetoDiagnostics.get_recent_veto_percentages",
    lambda *_, **__: np.array([60.0] + [0.0] * 31),
)
def test_run_state_vetoing():
    data = Data(
        start_time=2,
        stop_time=1,
    )

    assert data.run_state == RunState.VETOING


@patch(
    "kafka_dae_diagnostics.veto_diagnostics.VetoDiagnostics.get_recent_veto_percentages",
    lambda *_, **__: np.array([40.0] + [0.0] * 31),
)
def test_run_state_running():
    data = Data(
        start_time=2,
        stop_time=1,
    )

    assert data.run_state == RunState.RUNNING


def test_start_time_str():
    data = Data(
        start_time=0,
    )

    assert data.start_time_str == "N/A"

    data.start_time = 10000
    assert data.start_time_str.startswith("Thu 01-Jan-1970")


def test_stop_time_str():
    data = Data(
        stop_time=0,
    )

    assert data.stop_time_str == "N/A"

    data.stop_time = 10000
    assert data.stop_time_str.startswith("Thu 01-Jan-1970")


def test_binning_start():
    data = Data(linear_tcb_start_ns=200_000)
    assert data.linear_tcb_start_us == 200
    data.linear_tcb_start_us = 400
    assert data.linear_tcb_start_us == 400
    assert data.linear_tcb_start_ns == 400_000


def test_binning_end():
    data = Data(linear_tcb_end_ns=200_000)
    assert data.linear_tcb_end_us == 200
    data.linear_tcb_end_us = 400
    assert data.linear_tcb_end_us == 400
    assert data.linear_tcb_end_ns == 400_000
