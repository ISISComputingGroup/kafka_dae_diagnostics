"""Kafka DAE diagnostics."""
import dataclasses
import logging
import time
import uuid

import numpy as np
import numpy.typing as npt
from p4p.server import DynamicProvider, Server
from p4p.server.thread import SharedPV, Handler
from confluent_kafka import Consumer
from streaming_data_types.utils import get_schema
from streaming_data_types import deserialise_ev44

from kafka_dae_diagnostics._kdaediag_rs import (
    bin_events_into_spectrum,
    bin_events_into_spectrum_linear,
)

from p4p.nt import NTNDArray

logger = logging.getLogger(__name__)

logging.basicConfig(level=logging.DEBUG)


@dataclasses.dataclass
class Data:
    spectra: npt.NDArray[np.uint64]
    spectrum_updaters: list[tuple[int, int, SharedPV]]


class SpectrumHandler(Handler):
    def __init__(self, prefix: str, data: Data) -> None:
        self._data = data
        self._prefix = prefix

    def testChannel(self, name: str) -> bool:
        return name.startswith(self._prefix)

    def makeChannel(self, name: str, peer: str) -> SharedPV:
        logger.info(f"Making channel {name} {peer}")

        name = name[len(self._prefix):]
        period = 0
        det = int(name)

        data = self._data

        class SpectrumSharedPVHandler:
            def onLastDisconnect(self, pv):
                data.spectrum_updaters.remove((period, det, pv))

        pv = SharedPV(
            nt=NTNDArray(),
            initial=self._data.spectra[period, det].astype(np.double),
            handler=SpectrumSharedPVHandler()
        )

        self._data.spectrum_updaters.append((period, det, pv))
        return pv


def handle_ev44(data: Data, msg: bytes):
    ev44 = deserialise_ev44(msg)

    bin_events_into_spectrum(
        histogram=data.spectra[0],
        event_tofs=ev44.time_of_flight,
        pixel_ids=ev44.pixel_id,
        tof_bin_boundaries=np.linspace(0, 20_000_000, 1_000, dtype=np.int32)
    )


def handle_msg(data: Data, msg: bytes):
    schema = get_schema(msg)
    if schema == "ev44":
        handle_ev44(data, msg)


def main() -> None:
    data = Data(
        spectra=np.zeros(shape=(10, 1_000, 1_000), dtype=np.uint64),
        spectrum_updaters=[],
    )

    handler = SpectrumHandler("TE:NDW2922:KDAEDIAG:", data)
    providers = [
        DynamicProvider("spectra", handler=handler),
    ]
    server = Server(providers=providers)
    with server:
        consume_from_kafka_forever(data)


def consume_from_kafka_forever(data: Data) -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": "livedata.isis.cclrc.ac.uk:31092",
            "group.id": f"kafka-dae-diagnostics-{uuid.uuid4()}",
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe(["NDW2922_events"])

    while True:
        messages = consumer.consume(num_messages=100, timeout=0.1)
        for msg in messages:
            if msg.error():
                logger.warning("Kafka message error: %s", msg.error().code())
                continue
            handle_msg(data, msg.value())
            data.spectra[(0, 0, 0)] += 1

        if len(messages) > 0:
            # If any messages arrived, spectra may have changed - update any PVs who
            # are listening.
            for period, detector, pv in data.spectrum_updaters:
                pv.post(data.spectra[(0, detector)].astype(np.double), timestamp=time.time())

        print(len(data.spectrum_updaters))
