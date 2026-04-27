"""Utilities for reacting to Kafka messages."""

import logging
import time

import numpy as np
from confluent_kafka import Consumer, Message, TopicPartition
from streaming_data_types import deserialise_6s4t, deserialise_ev44, deserialise_pl72
from streaming_data_types.utils import get_schema

from kafka_dae_diagnostics._kdaediag_rs import bin_events_into_spectrum
from kafka_dae_diagnostics.data import Data

logger = logging.getLogger(__name__)


def handle_event_messages(event_messages: list[Message], data: Data) -> None:
    """Handle Kafka event messages.

    Args:
        event_messages: Messages received from Kafka event topic.
        data: Data served by ``kafka_dae_diagnostics``.

    """
    for msg in event_messages:
        if value := msg.value():
            handle_event_msg(data, value)
        elif error := msg.error():
            logger.warning("Kafka message error: %s", error.code())


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
    try:
        ev44 = deserialise_ev44(msg)
    except Exception:
        logger.exception("Failed deserialising ev44")
        return

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
    """Handle Kafka run info messages.

    Args:
        run_info_messages: Messages received from Kafka runInfo topic.
        data: Data served by ``kafka_dae_diagnostics``.
        event_consumer: Consumer for event Kafka topic.

    """
    logger.debug("Processing %s runInfo messages", len(run_info_messages))
    for msg in run_info_messages:
        if value := msg.value():
            handle_runinfo_msg(data, value, event_consumer)
        elif error := msg.error():
            logger.warning("Kafka message error: %s", error.code())
            continue


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
    elif schema == "6s4t":
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
    try:
        pl72 = deserialise_pl72(msg)
    except Exception:
        logger.exception("Failed deserialising pl72: %s")
        return

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

    topic = event_consumer.assignment()[0].topic
    partitions = event_consumer.list_topics(topic).topics[topic].partitions.keys()
    event_consumer.assign(
        event_consumer.offsets_for_times(
            [TopicPartition(topic, partition, pl72.start_time) for partition in partitions]
        )
    )

    pl72_timestamp_s = pl72.start_time / 1000

    data.total_events = 0
    data.total_event_messages = 0
    data.total_event_megabytes = 0
    data.largest_kafka_timestamp = pl72_timestamp_s
    data.most_recent_kafka_timestamp = pl72_timestamp_s
    data.start_time = pl72_timestamp_s


def handle_6s4t(data: Data, msg: bytes) -> None:
    """Handle a 6s4t (run stop) message from Kafka."""
    try:
        run_stop_6s4t = deserialise_6s4t(msg)
    except Exception:
        logger.exception("Failed deserialising 6s4t")
        return
    logger.info("Run stop (run_name=%s)", run_stop_6s4t.run_name)
    data.stop_time = run_stop_6s4t.stop_time / 1000
