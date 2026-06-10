"""Utilities for reacting to Kafka messages."""

import logging
from confluent_kafka import Consumer, Message, TopicPartition
from kafka_dae_diagnostics._kdaediag_rs import Data

logger = logging.getLogger(__name__)


def handle_event_topic_messages(event_messages: list[Message], data: Data) -> None:
    """Handle Kafka event messages.

    Args:
        event_messages: Messages received from Kafka event topic.
        data: Data served by ``kafka_dae_diagnostics``.

    """
    for msg in event_messages:
        if error := msg.error():
            logger.warning("Kafka message error: %s", error.code())
        elif value := msg.value():
            data.handle_msg(value, msg.partition() or 0)


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
        if error := msg.error():
            logger.warning("Kafka message error: %s", error.code())
        elif value := msg.value():
            old_run_start_time = data.start_time()
            data.handle_msg(value, msg.partition() or 0)
            new_start_time = data.start_time()

            if new_start_time != old_run_start_time:
                reassign_event_consumer(event_consumer, new_start_time)


def reassign_event_consumer(event_consumer: Consumer, start_time_s: float) -> None:
    """Reassign the event-stream consumer to start at a specified time.

    Args:
        event_consumer: Kafka event topic consumer.
        start_time_s: Start time of event consumer (in seconds since epoch).

    """
    topic = event_consumer.assignment()[0].topic
    partitions = event_consumer.list_topics(topic).topics[topic].partitions.keys()
    event_consumer.assign(
        event_consumer.offsets_for_times(
            [TopicPartition(topic, partition, int(start_time_s * 1000)) for partition in partitions]
        )
    )
