"""Kafka consumers."""

import logging
import time
import uuid

from confluent_kafka import Consumer, TopicPartition

from kafka_dae_diagnostics.data import Data
from kafka_dae_diagnostics.kafka.handlers import handle_event_messages, handle_run_info_messages

logger = logging.getLogger(__name__)


def make_runinfo_consumer(broker: str, topic: str) -> Consumer:
    """Make a runInfo consumer.

    This consumer will start reading from the 2 most recent messages on the
    runInfo topic; one of these messages should include the most recent run start
    (pl72) message, which will cause ``kafka_dae_diagnostics`` to correctly configure
    itself for the current (perhaps in-progress) run on startup.
    """
    settings = {
        "bootstrap.servers": broker,
        "group.id": f"kafka-dae-diagnostics-{uuid.uuid4()}",
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
        "fetch.max.bytes": 8 * 1024**2,  # 8MB
        "max.partition.fetch.bytes": 8 * 1024**2,  # 8MB
    }

    runinfo_consumer = Consumer(settings)

    low, high = runinfo_consumer.get_watermark_offsets(TopicPartition(topic, 0), cached=False)
    start_offset = max(high - 2, low)
    runinfo_consumer.assign([TopicPartition(topic, 0, start_offset)])
    return runinfo_consumer


def make_event_consumer(broker: str, topic: str) -> Consumer:
    """Make an event consumer."""
    settings = {
        "bootstrap.servers": broker,
        "group.id": f"kafka-dae-diagnostics-{uuid.uuid4()}",
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
        "fetch.max.bytes": 512 * 1024**2,  # 512MB
        "max.partition.fetch.bytes": 512 * 1024**2,  # 512MB
        "fetch.min.bytes": 64 * 1024**2,  # 64MB
        "fetch.wait.max.ms": 100,
        "statistics.interval.ms": 30000,
        "stats_cb": logger.debug,
    }

    event_consumer = Consumer(settings)
    partitions = event_consumer.list_topics(topic).topics[topic].partitions.keys()
    event_consumer.assign([TopicPartition(topic, partition) for partition in partitions])
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


def consume_from_kafka_forever(
    broker: str, run_info_topic: str, event_topic: str, data: Data, callback_frequency: float
) -> None:
    """Consume from Kafka forever.

    Args:
        broker: Kafka broker to connect to.
        run_info_topic: Name of the runInfo topic.
        event_topic: Name of the event topic.
        data: The data to serve.
        callback_frequency: How frequently to update PVs (s)

    """
    runinfo_consumer = make_runinfo_consumer(broker=broker, topic=run_info_topic)
    event_consumer = make_event_consumer(broker=broker, topic=event_topic)
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
        if now - last_callback_time > callback_frequency:
            run_callbacks(data)
            last_callback_time = now

        if len(event_messages) == 0:
            time.sleep(0.01)
