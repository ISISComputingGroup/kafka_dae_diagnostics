"""Helpers for autosaving a subset of parameters to a 'state' file."""

import json
import logging
from pathlib import Path
from typing import Any

from kafka_dae_diagnostics.data import Data

logger = logging.getLogger(__name__)

BINNING_START_NS_KEY = "binning_start_ns"
BINNING_END_NS_KEY = "binning_end_ns"
BINNING_NUM_POINTS_KEY = "binning_num_points"


def save_to_file(data: Data, state_file: Path) -> None:
    """Save parameters to an autosave ('state') file."""
    try:
        with state_file.open("w", encoding="utf-8") as f:
            json.dump(
                {
                    BINNING_START_NS_KEY: data.binning_start_ns,
                    BINNING_END_NS_KEY: data.binning_end_ns,
                    BINNING_NUM_POINTS_KEY: data.binning_num_points,
                },
                f,
                indent=2,
            )
    except Exception as e:  # ruff:ignore[blind-except]
        logger.error("Failed to save to state file %s - %s", state_file, e)


def load_from_file(state_file: Path) -> dict[str, Any]:
    """Load parameters from an autosave ('state') file."""
    try:
        with state_file.open(encoding="utf-8") as f:
            value = json.load(f)
            logger.info("Loaded parameters from %s: %s", state_file, value)
            return value
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logger.warning("Failed to load state file %s - %s.", state_file, e)
        return {}
