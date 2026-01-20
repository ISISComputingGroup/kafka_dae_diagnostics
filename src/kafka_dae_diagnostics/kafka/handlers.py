"""Utilities for reacting to Kafka messages."""

import logging
import time

import numpy as np
from confluent_kafka import Consumer, Message, TopicPartition
from streaming_data_types import deserialise_6s4t, deserialise_ev44, deserialise_pl72, serialise_ev44
from streaming_data_types.utils import get_schema

from kafka_dae_diagnostics._kdaediag_rs import bin_events_into_spectrum
from kafka_dae_diagnostics.data import Data

logger = logging.getLogger(__name__)


def handle_event_messages(event_messages: list[Message], data: Data) -> None:
    """Handle kafka event messages.

    Args:
        event_messages: Messages received from Kafka event topic.
        data: Data served by ``kafka_dae_diagnostics``.

    """
    t = time.time()
    s = 0
    for msg in event_messages:
        if value := msg.value():
            ti = time.time()
            handle_event_msg(data, value)
            s += time.time() - ti
        elif error := msg.error():
            logger.warning("Kafka message error: %s", error.code())
            continue
        else:
            logger.warning("Kafka event message neither error() nor value() available, ignoring.")
    print(f"total: {time.time() - t} sum: {s}")


def handle_event_msg(data: Data, msg: bytes) -> None:
    """Handle an arbitrary message from Kafka event topic.

    Args:
        data: Reference to data being served.
        msg: Message bytes received from Kafka.

    """
    schema = get_schema(msg)
    if schema == "ev44":
        handle_ev44(data, msg)


def handle_ev44(data: Data, msg: bytes) -> None:
    """Handle an ev44 (event-data) message from Kafka.

    Args:
        data: Reference to data being served.
        msg: Message bytes received from Kafka.

    """
    ev44 = deserialise_ev44(msg)
    bin_events_into_spectrum(
        histogram=data.spectra[0],
        event_tofs=ev44.time_of_flight,
        pixel_ids=ev44.pixel_id,
        tof_bin_boundaries=data.bin_boundaries,
    )

    data.total_events += ev44.pixel_id.size
    data.total_event_messages += 1
    data.total_event_megabytes += len(msg) / 1024**2

    ev44_timestamp_s = ev44.reference_time[0] / 1_000_000_000
    data.largest_kafka_timestamp = max(data.largest_kafka_timestamp, ev44_timestamp_s)
    data.most_recent_kafka_timestamp = ev44_timestamp_s
    data.event_processing_lag = max(time.time() - ev44_timestamp_s, 0)


def handle_run_info_messages(
    run_info_messages: list[Message], data: Data, event_consumer: Consumer
) -> None:
    """Handle kafka event messages.

    Args:
        run_info_messages: Messages received from Kafka runInfo topic.
        data: Data served by ``kafka_dae_diagnostics``.
        event_consumer: Consumer for event Kafka topic.

    """
    logger.debug("Processing %s runInfo messages", len(run_info_messages))
    for msg in run_info_messages:
        if error := msg.error():
            logger.warning("Kafka message error: %s", error.code())
            continue
        elif value := msg.value():
            handle_runinfo_msg(data, value, event_consumer)
        else:
            logger.warning("Kafka runInfo message neither error() nor value() available, ignoring.")


def handle_runinfo_msg(data: Data, msg: bytes, event_consumer: Consumer) -> None:
    """Handle an arbitrary message from Kafka runInfo topic.

    Args:
        data: Reference to data being served.
        msg: Message bytes received from Kafka.
        event_consumer: Kafka event topic consumer.

    """
    schema = get_schema(msg)
    if schema == "pl72":
        handle_pl72(data, msg, event_consumer)
    if schema == "6s4t":
        handle_6s4t(data, msg)


def handle_pl72(data: Data, msg: bytes, event_consumer: Consumer) -> None:
    """Handle a pl72 (run start) message from Kafka.

    This zeroes the spectra array (reallocating if the size changed),
    and configures the event consumer to re-read all events since run
    start (in case run start timestamp was in the past).

    Args:
        data: Reference to data being served.
        msg: Message bytes received from Kafka.
        event_consumer: Kafka event topic consumer.

    """
    pl72 = deserialise_pl72(msg)

    det_spec_map = pl72.detector_spectrum_map
    if det_spec_map is None:
        n_spectra = 1
    else:
        n_spectra = det_spec_map.n_spectra

    logger.info(
        "Run start (filename='%s', start_time='%i', "
        "run_name='%s', instrument-name='%s', n_spectra='%i')",
        pl72.filename,
        pl72.start_time,
        pl72.run_name,
        pl72.instrument_name,
        n_spectra,
    )
    periods = 1  # TODO
    detectors = n_spectra
    time_channels = 1000  # TODO

    data.bin_boundaries = np.linspace(0, 20_000_000, time_channels + 1, dtype=np.int32)  # TODO

    # Only reallocate if shape has changed - otherwise zero existing array.
    if data.spectra.shape == (periods, detectors, time_channels):
        data.spectra[...] = 0
    else:
        del data.spectra
        data.spectra = np.zeros((periods, detectors, time_channels), dtype=np.float64)

    # Assign event consumer to start at the time the run started
    tp = event_consumer.assignment()[0]
    event_consumer.seek(
        event_consumer.offsets_for_times([TopicPartition(tp.topic, tp.partition, pl72.start_time)])[
            0
        ]
    )

    pl72_timestamp_s = pl72.start_time / 1000

    data.total_events = 0
    data.total_event_messages = 0
    data.total_event_megabytes = 0
    data.largest_kafka_timestamp = pl72_timestamp_s
    data.most_recent_kafka_timestamp = pl72_timestamp_s
    data.start_time = pl72_timestamp_s


def handle_6s4t(data: Data, msg: bytes) -> None:
    """Handle a 6s4t (run stop) mesage from Kafka."""
    run_stop_6s4t = deserialise_6s4t(msg)
    data.stop_time = run_stop_6s4t.stop_time / 1000


RNG = np.random.default_rng()


def generate_fake_events(
    msg_id: int,
    events_per_frame: int,
    tof_peak: float,
    tof_sigma: float,
    det_min: int,
    det_max: int,
) -> bytes:
    detector_ids = RNG.integers(low=det_min, high=det_max, size=events_per_frame)
    tofs = np.maximum(0.0, RNG.normal(loc=tof_peak, scale=tof_sigma, size=events_per_frame))

    return serialise_ev44(
        source_name="saluki",
        reference_time=[time.time() * 1_000_000_000],
        message_id=msg_id,
        reference_time_index=[0],
        time_of_flight=tofs,
        pixel_id=detector_ids,
    )


if __name__ == "__main__":

    num = 5000

    n_events = 26_000
    data = Data(
        spectra=np.zeros((1, 50_000, 1000), dtype=np.float64),
        bin_boundaries=np.linspace(0, 20_000_000, 1001, dtype=np.int32),
    )
    spec = data.spectra[0][:]
    msgs = [
        generate_fake_events(0, n_events, 10_000_000, 2_000_000, 0, 50_000)
        for _ in range(num)
    ]
    len_bytes = sum(len(msg) for msg in msgs)

    start = time.time()
    for msg in msgs:
        handle_ev44(data, msg)

    t = (time.time() - start)

    print(f"{t*1000:.3f} ms")
    print(f"{t*1000/len(msgs):.6f} ms/msg")
    print(f"{len_bytes / (1024 * 1024 * t):.3f} MiB/s")
    print(f"{len_bytes * 8 / (1024 * 1024 * t):.3f} Mbit/s")
    print(f"{n_events * num / (1_000_000 * t):.3f} Mev/s")
