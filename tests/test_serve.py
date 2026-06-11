from unittest import mock
from unittest.mock import patch

from kafka_dae_diagnostics.config import DiagnosticsConfig
from kafka_dae_diagnostics.serve import serve
from kafka_dae_diagnostics.veto_diagnostics import NUM_VETOS


def test_serve():
    with patch("kafka_dae_diagnostics.serve.consume_from_kafka_forever") as consume:
        config = DiagnosticsConfig(
            pv_prefix="UNITTEST:",
            runinfo_topic="unittest_runInfo",
            events_topic="unittest_events",
            callback_frequency_ms=1000,
            kafka_events_consumer={},
            kafka_runinfo_consumer={},
            veto_names=[f"v{n}" for n in range(NUM_VETOS)],
        )
        serve(config)

        consume.assert_called_once_with(
            config=config,
            data=mock.ANY,
        )
