"""Kafka consumers."""

import logging
import time

from confluent_kafka import Consumer, TopicPartition

from kafka_dae_diagnostics.config import DiagnosticsConfig
from kafka_dae_diagnostics.data import Data
from kafka_dae_diagnostics.kafka.handlers import handle_event_messages, handle_run_info_messages

logger = logging.getLogger(__name__)


def make_runinfo_consumer(config: DiagnosticsConfig) -> Consumer:
    """Make a runInfo consumer.

    This consumer will start reading from the 2 most recent messages on the
    runInfo topic; one of these messages should include the most recent run start
    (pl72) message, which will cause ``kafka_dae_diagnostics`` to correctly configure
    itself for the current (perhaps in-progress) run on startup.
    """
    runinfo_consumer = Consumer(config.kafka_runinfo_consumer)

    low, high = runinfo_consumer.get_watermark_offsets(
        TopicPartition(config.runinfo_topic, 0), cached=False
    )
    start_offset = max(high - 2, low)
    runinfo_consumer.assign([TopicPartition(config.runinfo_topic, 0, start_offset)])
    return runinfo_consumer


def make_event_consumer(config: DiagnosticsConfig) -> Consumer:
    """Make an event consumer."""
    event_consumer = Consumer(config.kafka_events_consumer)
    partitions = (
        event_consumer.list_topics(config.events_topic)
        .topics[config.events_topic]
        .partitions.keys()
    )
    event_consumer.assign(
        [TopicPartition(config.events_topic, partition) for partition in partitions]
    )
    return event_consumer


def run_callbacks(data: Data) -> None:
    """Run all callbacks with updated data.

    These callbacks will push new values to PVs.

    Args:
        data: The data to serve

    """
    with data.callbacks_lock:
        for callback_id, cb in data.callbacks.items():
            try:
                cb()
            except Exception as e:  # noqa: BLE001
                logger.warning(
                    "Callback '%s' failed, error: %s %s", callback_id, e.__class__.__name__, e
                )


def consume_from_kafka_forever(config: DiagnosticsConfig, data: Data) -> None:
    """Consume from Kafka forever.

    Args:
        config: Diagnostics IOC configuration parameters
        data: The data to serve.

    """
    runinfo_consumer = make_runinfo_consumer(config)
    event_consumer = make_event_consumer(config)
    last_callback_time = 0

    while True:
        run_info_messages = runinfo_consumer.consume(num_messages=100, timeout=0.0)
        if run_info_messages:
            handle_run_info_messages(run_info_messages, data=data, event_consumer=event_consumer)

        event_messages = event_consumer.consume(num_messages=10_000, timeout=0.0)
        if event_messages:
            handle_events_start_time = time.time()
            handle_event_messages(event_messages, data=data)
            time_ms = (time.time() - handle_events_start_time) * 1000
            logger.debug(
                "Handled %d event messages in %.3f ms (%.3f ms per message).",
                len(event_messages),
                time_ms,
                time_ms / len(event_messages),
            )

        now = time.time()
        if (now - last_callback_time) * 1000 > config.callback_frequency_ms:
            run_callbacks(data)
            last_callback_time = now

        if len(event_messages) == 0:
            time.sleep(0.01)
