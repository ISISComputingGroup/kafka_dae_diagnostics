"""
Utilities for reacting to Kafka messages.
"""
import logging
import time

import numpy.typing as npt

import numpy as np
from confluent_kafka import TopicPartition, Message, Consumer
from streaming_data_types.utils import get_schema
from streaming_data_types import deserialise_ev44, deserialise_pl72

from kafka_dae_diagnostics.data import Data
from kafka_dae_diagnostics._kdaediag_rs import bin_events_into_spectrum


logger = logging.getLogger(__name__)


def handle_event_messages(event_messages: list[Message], data: Data) -> None:
    """
    Handle kafka event messages.

    Args:
        event_messages: Messages received from Kafka event topic.
        data: Data served by ``kafka_dae_diagnostics``.
    """
    for msg in event_messages:
        if msg.error():
            logger.warning("Kafka message error: %s", msg.error().code())
            continue
        handle_event_msg(data, msg.value())


def handle_event_msg(data: Data, msg: bytes):
    """Handle an arbitrary message from Kafka event topic.

    Args:
        data: Reference to data being served.
        msg: Message bytes received from Kafka.
    """
    schema = get_schema(msg)
    if schema == "ev44":
        handle_ev44(data, msg)


def handle_ev44(data: Data, msg: bytes):
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
        tof_bin_boundaries=data.bin_boundaries
    )

    data.total_events += ev44.pixel_id.size
    data.total_event_messages += 1
    data.total_event_megabytes += len(msg) / 1024**2

    ev44_timestamp_s = ev44.reference_time[0] / 1_000_000_000
    data.largest_kafka_timestamp = max(data.largest_kafka_timestamp, ev44_timestamp_s)
    data.most_recent_kafka_timestamp = ev44_timestamp_s
    data.processing_lag = max(time.time() - ev44_timestamp_s, 0)


def handle_run_info_messages(run_info_messages: list[Message], data: Data, event_consumer: Consumer):
    """
    Handle kafka event messages.

    Args:
        run_info_messages: Messages received from Kafka runInfo topic.
        data: Data served by ``kafka_dae_diagnostics``.
        event_consumer: Consumer for event Kafka topic.
    """
    logger.debug("Processing %s runInfo messages", len(run_info_messages))
    for msg in run_info_messages:
        if msg.error():
            logger.warning("Kafka message error: %s", msg.error().code())
            continue
        handle_runinfo_msg(data, msg.value(), event_consumer)


def handle_runinfo_msg(data: Data, msg: bytes, event_consumer: Consumer) -> int | None:
    """Handle an arbitrary message from Kafka runInfo topic.

    Args:
        data: Reference to data being served.
        msg: Message bytes received from Kafka.
        event_consumer: Kafka event topic consumer.
    """
    schema = get_schema(msg)
    if schema == "pl72":
        handle_pl72(data, msg, event_consumer)


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
    logger.info("Run start (filename='%s', start_time='%i', run_name='%s', instrument-name='%s', n_spectra='%i')", pl72.filename, pl72.start_time, pl72.run_name, pl72.instrument_name, pl72.detector_spectrum_map.n_spectra)
    periods = 1  # TODO
    detectors = pl72.detector_spectrum_map.n_spectra
    time_channels =  1000  # TODO

    data.time_bin_boundaries = np.linspace(0, 20_000_000, time_channels+1, dtype=np.int32)  # TODO

    # Only reallocate if shape has changed - otherwise zero existing array.
    if data.spectra.shape == (periods, detectors, time_channels):
        data.spectra[...] = 0
    else:
        del data.spectra
        data.spectra = np.zeros([periods, detectors, time_channels], dtype=np.float64)

    # Assign event consumer to start at the time the run started
    tp = event_consumer.assignment()[0]
    event_consumer.seek(event_consumer.offsets_for_times(
        [TopicPartition(tp.topic, tp.partition, pl72.start_time)]
    )[0])

    pl72_timestamp_s = pl72.start_time / 1000

    data.total_events = 0
    data.total_event_messages = 0
    data.total_event_megabytes = 0
    data.largest_kafka_timestamp = pl72_timestamp_s
    data.most_recent_kafka_timestamp = pl72_timestamp_s
    data.start_time = pl72_timestamp_s
