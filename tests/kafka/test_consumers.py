from unittest import mock
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka import TopicPartition

from kafka_dae_diagnostics.config import DiagnosticsConfig
from kafka_dae_diagnostics.data import Data
from kafka_dae_diagnostics.kafka.consumers import (
    consume_from_kafka_forever,
    make_event_consumer,
    make_runinfo_consumer,
    run_callbacks,
)


def test_make_runinfo_consumer():
    topic = "someRandomTopic"

    with patch("kafka_dae_diagnostics.kafka.consumers.Consumer") as mock_consumer:
        mock_consumer.return_value.get_watermark_offsets.return_value = (1234, 4321)
        make_runinfo_consumer(
            DiagnosticsConfig(
                runinfo_topic=topic,
                kafka_runinfo_consumer={},
                kafka_events_consumer={},
                pv_prefix="",
                events_topic="",
                callback_frequency_ms=1000,
            )
        )

        mock_consumer.return_value.assign.assert_called_once_with([TopicPartition(topic, 0, 4319)])


def test_make_event_consumer():
    topic = "someRandomTopic"

    with patch("kafka_dae_diagnostics.kafka.consumers.Consumer") as mock_consumer:
        mock_consumer.return_value.get_watermark_offsets.return_value = (1234, 4321)
        make_event_consumer(
            DiagnosticsConfig(
                runinfo_topic="",
                events_topic=topic,
                kafka_runinfo_consumer={},
                kafka_events_consumer={},
                pv_prefix="",
                callback_frequency_ms=1000,
            )
        )

        mock_consumer.return_value.assign.assert_called_once()


def test_run_callbacks():
    cb1 = MagicMock()
    cb2 = MagicMock(side_effect=ValueError)
    cb3 = MagicMock()

    data = Data(
        callbacks={"one": cb1, "two": cb2, "three": cb3},
    )

    run_callbacks(data)

    cb1.assert_called_once_with()
    cb2.assert_called_once_with()
    cb3.assert_called_once_with()


def test_consume_from_kafka_forever():
    data = Data()
    with (
        patch(
            "kafka_dae_diagnostics.kafka.consumers.make_runinfo_consumer"
        ) as make_runinfo_consumer,
        patch("kafka_dae_diagnostics.kafka.consumers.make_event_consumer") as make_event_consumer,
        patch(
            "kafka_dae_diagnostics.kafka.consumers.handle_run_info_messages"
        ) as handle_run_info_messages,
        patch(
            "kafka_dae_diagnostics.kafka.consumers.handle_event_messages"
        ) as handle_event_messages,
        patch("kafka_dae_diagnostics.kafka.consumers.run_callbacks") as run_callbacks,
        patch("kafka_dae_diagnostics.kafka.consumers.time.sleep") as sleep,
    ):
        make_runinfo_consumer.return_value.consume.side_effect = [[b"some_runinfo_message"], []]
        make_event_consumer.return_value.consume.side_effect = [[b"some_event_message"], []]

        # Slightly ugly way to force the infinite loop to break when it has
        # nothing more to process.
        sleep.side_effect = TimeoutError("Waiting for new events")

        config = DiagnosticsConfig(
            runinfo_topic="runInfo",
            events_topic="events",
            kafka_runinfo_consumer={},
            kafka_events_consumer={},
            pv_prefix="",
            callback_frequency_ms=1000,
        )

        with pytest.raises(TimeoutError, match="Waiting for new events"):
            consume_from_kafka_forever(config=config, data=data)

        handle_run_info_messages.assert_called_once_with(
            [b"some_runinfo_message"], data=mock.ANY, event_consumer=mock.ANY
        )
        handle_event_messages.assert_called_once_with([b"some_event_message"], data=mock.ANY)
        run_callbacks.assert_called()
