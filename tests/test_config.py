from unittest.mock import mock_open, patch

import pytest

from kafka_dae_diagnostics.config import load_config


def test_config_loading():
    m = mock_open(
        read_data=b"""
# PV Prefix of all PVs on this IOC.
pv_prefix = "TE:TESTMACHINE:KDAEDIAG:"

# How often (milliseconds) to update EPICS PVs.
callback_frequency_ms = 250

# Kafka topic on which to listen for runInfo messages
# (usually <instrument>_runInfo)
runinfo_topic = "somemachine_runInfo"
# Kafka topic on which to listen for event messages
# (usually <instrument>_events)
events_topic = "somemachine_events"

vetoconfig_topic = "somemachine_vetoConfig"

[kafka_runinfo_consumer]
"bootstrap.servers" = "server:1234"

[kafka_events_consumer]
"bootstrap.servers" = "server:4321"

[kafka_vetoconfig_consumer]
"bootstrap.servers" = "server:4231"
"""
    )

    with patch("kafka_dae_diagnostics.config.open", m):
        config = load_config("")

    assert config.runinfo_topic == "somemachine_runInfo"
    assert config.events_topic == "somemachine_events"
    assert config.vetoconfig_topic == "somemachine_vetoConfig"
    assert config.callback_frequency_ms == 250
    assert config.kafka_events_consumer == {"bootstrap.servers": "server:4321"}
    assert config.kafka_runinfo_consumer == {"bootstrap.servers": "server:1234"}
    assert config.kafka_vetoconfig_consumer == {"bootstrap.servers": "server:4231"}


def test_invalid_config_loading():
    m = mock_open(
        read_data=b"""
pv_prefix = 2
# Lots of missing keys
"""
    )

    with (
        patch("kafka_dae_diagnostics.config.open", m),
        pytest.raises(ValueError, match=r"Unable to load config .*"),
    ):
        load_config("")
