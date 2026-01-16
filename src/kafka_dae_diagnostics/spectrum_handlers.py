""":py:obj:`p4p.server.raw.Handler` types for spectrum PVs."""

import logging
import re
import uuid

import numpy as np
from p4p.server.raw import Handler

from kafka_dae_diagnostics.data import Data
from p4p.server.thread import SharedPV

from p4p.nt import NTScalar

logger = logging.getLogger(__name__)


class SpectrumHandler(Handler):
    """Handler for Spectrum Y (counts) data.
    """

    def __init__(self, prefix: str, data: Data) -> None:
        """Handler for Spectrum Y (counts) data.

        Args:
            prefix: PV prefix (e.g. ``IN:INSTNAME:KDAEDIAG:``)
            data: Reference to data being served.
        """
        self._data = data
        self._prefix = prefix
        self._channel_regex = re.compile(rf"^{re.escape(prefix)}SPEC:(\d+):(\d+):([XY])$")

    def testChannel(self, name: str) -> bool | str:
        """
        Test whether a channel with the given name can be served by this handler.

        Args:
            name: Channel name.
        """
        match = self._channel_regex.fullmatch(name)
        return match is not None \
            and int(match.group(1)) < self._data.num_periods \
            and int(match.group(2)) < self._data.num_detectors

    def makeChannel(self, name: str, peer: str) -> SharedPV:
        """
        Make a channel for the PV with the given name.

        Args:
            name: Channel name.
            peer: remote address.
        """
        logger.info(f"Opening channel {name} {peer}")

        match = self._channel_regex.fullmatch(name)
        period = int(match.group(1))
        det = int(match.group(2))
        typ = match.group(3)

        callback_id = f"{name}#{uuid.uuid4()}"

        def extract_data(data: Data):
            match typ:
                case "Y":
                    return self._data.spectra[period][det]
                case "X":
                    return ((self._data.bin_boundaries[1:] + self._data.bin_boundaries[:-1]) / 2).astype(np.float64)
                case _:
                    raise ValueError(f"Unknown channel type: {typ}")

        class ConnectionHandler:
            @staticmethod
            def onLastDisconnect(*_, **__):
                logger.info(f"Closing channel {name} {peer}")
                with self._data.callbacks_lock:
                    del self._data.callbacks[callback_id]

        pv = SharedPV(
            nt=NTScalar("ad"),
            initial=extract_data(self._data),
            handler=ConnectionHandler(),
        )

        with self._data.callbacks_lock:
            self._data.callbacks[callback_id] = lambda data: pv.post(extract_data(data))

        return pv
