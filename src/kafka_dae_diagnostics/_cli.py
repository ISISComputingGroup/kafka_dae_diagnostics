"""Kafka DAE diagnostics."""

import argparse
import logging
import os

from kafka_dae_diagnostics.config import load_config
from kafka_dae_diagnostics.serve import serve

logger = logging.getLogger(__name__)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--config",
        type=str,
        required=True,
        help="Path to config file.",
    )
    ap.add_argument(
        "--log-level",
        default="INFO",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level.",
    )
    args = ap.parse_args()

    logging.basicConfig(level=args.log_level)

    if "EPICS_PVAS_INTF_ADDR_LIST" not in os.environ:
        logger.warning(
            "EPICS_PVAS_INTF_ADDR_LIST environment variable not set; "
            "IOC may not bind to expected network interfaces."
        )

    config = load_config(args.config)

    serve(config)
