"""Kafka DAE diagnostics."""

import argparse
import logging

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
        help="Path to config file.",
    )
    args = ap.parse_args()

    logging.basicConfig(level=args.log_level)

    config = load_config(args.config)

    serve(config)
