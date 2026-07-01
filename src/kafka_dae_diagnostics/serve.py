"""Main loop of ``kafka_dae_diagnostics``.

Listen to Kafka forever, updating ``data`` as new messages come in,
while also serving ``data`` over EPICS.
"""

from p4p.server import DynamicProvider, Server

from kafka_dae_diagnostics._kdaediag_rs import Data
from kafka_dae_diagnostics.config import DiagnosticsConfig
from kafka_dae_diagnostics.kafka.consumers import consume_from_kafka_forever
from kafka_dae_diagnostics.pvs.callbacks import Callbacks
from kafka_dae_diagnostics.pvs.spectrum_handlers import SpectrumHandler
from kafka_dae_diagnostics.pvs.static_pvs import static_pv_provider


def serve(
    config: DiagnosticsConfig,
) -> None:
    """Serve PVs while consuming from Kafka (forever).

    Args:
        config: Diagnostics IOC configuration parameters

    """
    data = Data()
    callbacks = Callbacks()
    spectrum_handler = SpectrumHandler(prefix=config.pv_prefix, data=data, callbacks=callbacks)

    providers = [
        DynamicProvider("spectra", handler=spectrum_handler),
        static_pv_provider(prefix=config.pv_prefix, data=data, callbacks=callbacks),
    ]

    server = Server(providers=providers)
    with server:
        consume_from_kafka_forever(
            config=config,
            data=data,
            callbacks=callbacks,
        )
