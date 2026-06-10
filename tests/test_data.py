import numpy as np
import pytest
from kafka_dae_diagnostics._kdaediag_rs import Data


@pytest.fixture
def data():
    return Data()


def test_mev(data):
    data.total_events = 5_600_000
    assert data.mev() == pytest.approx(5.6)


def test_duration(data):
    data.largest_kafka_timestamp = 20
    data.start_time = 5
    assert data.duration() == pytest.approx(15)


def test_negative_duration(data):
    data.largest_kafka_timestamp = 20
    data.start_time = 25
    assert data.duration() == pytest.approx(0)


def test_mev_per_hour(data):
    data.largest_kafka_timestamp = 3600
    data.start_time = 0
    data.total_events = 5_600_000
    assert data.mev_per_hour() == pytest.approx(5.6)

    data.start_time = 3600
    assert data.mev_per_hour() == pytest.approx(0.0)


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


def test_average_data_rate(data):
    data.total_event_megabytes = 123.456
    data.start_time = 0
    data.largest_kafka_timestamp = 2
    assert data.average_data_rate() == pytest.approx(123.456 / 2)

    data.largest_kafka_timestamp = 0
    assert data.average_data_rate() == pytest.approx(0)


def test_count_rate(data):
    data.total_events = 123456789
    data.start_time = 123
    data.largest_kafka_timestamp = 456

    assert data.count_rate() == pytest.approx(1334.667989)
