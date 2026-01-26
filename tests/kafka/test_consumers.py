from unittest.mock import MagicMock, patch

from confluent_kafka import TopicPartition

from kafka_dae_diagnostics.data import Data
from kafka_dae_diagnostics.kafka.consumers import (
    make_event_consumer,
    make_runinfo_consumer,
    run_callbacks,
)


def test_make_runinfo_consumer():
    topic = "someRandomTopic"

    with patch("kafka_dae_diagnostics.kafka.consumers.Consumer") as mock_consumer:
        mock_consumer.return_value.get_watermark_offsets.return_value = (1234, 4321)
        make_runinfo_consumer("127.0.0.1", topic)

        mock_consumer.return_value.assign.assert_called_once_with([TopicPartition(topic, 0, 4319)])


def test_make_event_consumer():
    topic = "someRandomTopic"

    with patch("kafka_dae_diagnostics.kafka.consumers.Consumer") as mock_consumer:
        mock_consumer.return_value.get_watermark_offsets.return_value = (1234, 4321)
        make_event_consumer("127.0.0.1", topic)

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
