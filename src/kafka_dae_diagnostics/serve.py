"""Main loop of ``kafka_dae_diagnostics``.

Listen to Kafka forever, updating ``data`` as new messages come in,
while also serving ``data`` over EPICS.
"""

from p4p.server import DynamicProvider, Server

from kafka_dae_diagnostics.data import Data
from kafka_dae_diagnostics.kafka.consumers import consume_from_kafka_forever
from kafka_dae_diagnostics.pvs.spectrum_handlers import SpectrumHandler
from kafka_dae_diagnostics.pvs.static_pvs import static_pv_provider


def serve(
    prefix: str, broker: str, run_info_topic: str, event_topic: str, callback_frequency: float
) -> None:
    """Serve PVs while consuming from Kafka (forever).

    Args:
        prefix: The PV Prefix of this IOC.
        broker: Kafka broker URL.
        run_info_topic: runInfo topic name.
        event_topic: event topic name.
        callback_frequency: PV update callback frequency (s).

    """
    data = Data()
    spectrum_handler = SpectrumHandler(prefix=prefix, data=data)
    providers = [
        DynamicProvider("spectra", handler=spectrum_handler),
        static_pv_provider(prefix=prefix, data=data),
    ]

    server = Server(providers=providers)
    with server:
        consume_from_kafka_forever(
            broker=broker,
            run_info_topic=run_info_topic,
            event_topic=event_topic,
            data=data,
            callback_frequency=callback_frequency,
        )
