from unittest import mock
from unittest.mock import patch

from kafka_dae_diagnostics.serve import serve


def test_serve():
    with patch("kafka_dae_diagnostics.serve.consume_from_kafka_forever") as consume:
        serve(
            prefix="UNITTEST:",
            broker="localhost:9092",
            run_info_topic="unittest_runInfo",
            event_topic="unittest_events",
            callback_frequency=0.1,
        )

        consume.assert_called_once_with(
            broker="localhost:9092",
            run_info_topic="unittest_runInfo",
            event_topic="unittest_events",
            data=mock.ANY,
            callback_frequency=0.1,
        )
