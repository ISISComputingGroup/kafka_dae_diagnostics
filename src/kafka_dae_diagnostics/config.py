"""Utilities for reading Diagnostics IOC configuration from TOML."""

import tomllib

from pydantic import BaseModel, PositiveFloat, ValidationError


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

    kafka_runinfo_consumer: dict[str, str]
    """Kafka settings for ``runInfo`` stream consumer."""

    kafka_events_consumer: dict[str, str]
    """Kafka settings for ``event`` stream consumer."""


def load_config(config_path: str) -> DiagnosticsConfig:
    """Validate and load a config file at the specified path."""
    with open(config_path, "rb") as f:
        data = tomllib.load(f)

    try:
        return DiagnosticsConfig.model_validate(data)
    except ValidationError as e:
        raise ValueError(f"Unable to load config from '{config_path}':\n{e}") from e
