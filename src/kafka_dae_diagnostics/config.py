"""Utilities for reading Diagnostics IOC configuration from TOML."""

import tomllib
from typing import Annotated

from pydantic import BaseModel, Field, PositiveFloat, ValidationError

from kafka_dae_diagnostics.veto_diagnostics import NUM_VETOS


class DiagnosticsConfig(BaseModel):
    """Configuration parameters for Kafka DAE diagnostics."""

    pv_prefix: str
    """PV prefix of all PVs on this IOC."""

    callback_frequency_ms: PositiveFloat
    """How often (milliseconds) to update EPICS PVs."""

    runinfo_topic: str
    """Kafka topic on which to listen for ``runInfo`` messages."""

    events_topic: str
    """Kafka topic on which to listen for ``event`` messages."""

    vetoconfig_topic: str
    """Kafka topic on which to listen for ``vetoConfig`` messages."""

    kafka_runinfo_consumer: dict[str, str]
    """Kafka settings for ``runInfo`` stream consumer."""

    kafka_events_consumer: dict[str, str]
    """Kafka settings for ``event`` stream consumer."""

    kafka_vetoconfig_consumer: dict[str, str]
    """Kafka settings for ``vetoConfig`` stream consumer."""

    min_vetoing_percentage: float = 50.0
    """
    The minimum active percentage (for each individual veto), beyond which the
    run state will be VETOING rather than RUNNING.
    """

    stale_event_message_timeout_s: float = 5.0
    """When running, if we have not received messages on the _events stream within
    this many seconds from now, the run state will be PROCESSING rather than RUNNING.
    """


def load_config(config_path: str) -> DiagnosticsConfig:
    """Validate and load a config file at the specified path."""
    with open(config_path, "rb") as f:
        data = tomllib.load(f)

    try:
        return DiagnosticsConfig.model_validate(data)
    except ValidationError as e:
        raise ValueError(f"Unable to load config from '{config_path}':\n{e}") from e
