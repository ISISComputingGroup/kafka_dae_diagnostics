"""Kafka DAE diagnostics."""

import argparse
import logging

from kafka_dae_diagnostics.serve import serve

logger = logging.getLogger(__name__)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--pv-prefix",
        type=str,
        required=True,
        help="PV Prefix including IOC name and trailing colon.",
    )
    ap.add_argument("--broker", type=str, required=True, help="Kafka broker URL and port")
    ap.add_argument("--event-topic", type=str, required=True, help="Kafka event topic name")
    ap.add_argument("--runinfo-topic", type=str, required=True, help="Kafka runInfo topic name")
    ap.add_argument(
        "--callback-frequency",
        type=float,
        default=0.1,
        help="Max rate at which to update diagnostic PVs (including spectra)",
    )
    args = ap.parse_args()

    logging.basicConfig(level=logging.DEBUG)

    serve(
        prefix=args.pv_prefix,
        broker=args.broker,
        event_topic=args.event_topic,
        run_info_topic=args.runinfo_topic,
        callback_frequency=args.callback_frequency,
    )
