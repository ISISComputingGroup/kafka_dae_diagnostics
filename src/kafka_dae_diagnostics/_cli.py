"""Kafka DAE diagnostics."""
import logging
import time
import uuid
from typing import Any

import numpy as np
from confluent_kafka.cimpl import TopicPartition
from p4p.server import DynamicProvider, Server
from confluent_kafka import Consumer

from kafka_dae_diagnostics.data import Data
from kafka_dae_diagnostics.kafka_handlers import handle_event_messages, \
    handle_run_info_messages, handle_event_msg
from kafka_dae_diagnostics.spectrum_handlers import SpectrumHandler

logger = logging.getLogger(__name__)

logging.basicConfig(level=logging.DEBUG)


def main() -> None:
    data = Data(
        spectra=np.zeros(shape=(1, 1, 1), dtype=np.uint64),
        spectrum_updaters=[],
    )

    handler = SpectrumHandler("TE:NDW2922:KDAEDIAG:", data)
    providers = [
        DynamicProvider("spectra", handler=handler),
    ]
    server = Server(providers=providers)
    with server:
        consume_from_kafka_forever(data)


def update_spectra(data: Data) -> None:
    num_updaters = len(data.spectrum_updaters)
    if num_updaters == 0:
        return

    logger.debug("Updating %d spectra for connected clients", num_updaters)
    for period, detector, pv in data.spectrum_updaters:
        try:
            pv.post(data.spectra[(0, detector)].astype(np.double), timestamp=time.time())
        except Exception as e:
            logger.warning("Failed to update dynamic spectrum PV for period %d, detector %d, error: %s %s", period,
                           detector, e.__class__.__name__, e)


def make_runinfo_consumer(settings: dict[str, Any]) -> Consumer:
    """
    Make a runInfo consumer.

    This consumer will start reading from the 2 most recent messages on the
    runInfo topic; one of these messages should include the most recent run start
    (pl72) message, which will cause ``kafka_dae_diagnostics`` to correctly configure
    itself for the current (perhaps in-progress) run on startup.
    """
    runinfo_consumer = Consumer(settings)

    start_offset = (
        runinfo_consumer.get_watermark_offsets(TopicPartition("NDW2922_runInfo", 0), cached=False)[1]
        - 2
    )
    runinfo_consumer.assign([TopicPartition("NDW2922_runInfo", 0, start_offset)])
    return runinfo_consumer


def make_event_consumer(settings: dict[str, Any]) -> Consumer:
    """Make an event consumer."""
    event_consumer = Consumer(settings)
    event_consumer.assign([TopicPartition("NDW2922_events", 0)])
    return event_consumer


def consume_from_kafka_forever(data: Data) -> None:
    group_id = f"kafka-dae-diagnostics-{uuid.uuid4()}"
    logger.info("Kafka group ID: %s", group_id)

    settings = {
        "bootstrap.servers": "livedata.isis.cclrc.ac.uk:31092",
        "group.id": group_id,
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
        "fetch.max.bytes": 512 * 1024 ** 2,  # 512MB
        "max.partition.fetch.bytes": 512 * 1024 ** 2,  # 512MB
    }

    runinfo_consumer = make_runinfo_consumer(settings)
    event_consumer = make_event_consumer(settings)

    while True:
        run_info_messages = runinfo_consumer.consume(num_messages=50, timeout=0.)
        handle_run_info_messages(run_info_messages, data=data, event_consumer=event_consumer)

        event_messages = event_consumer.consume(num_messages=1000, timeout=0.1)
        handle_event_messages(event_messages, data=data)

        if len(event_messages) > 0 or len(run_info_messages) > 0:
            # If any messages arrived, spectra may have changed - update any PVs which
            # are connected.
            update_spectra(data)
