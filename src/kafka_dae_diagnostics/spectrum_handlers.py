""":py:obj:`p4p.server.raw.Handler` types for spectrum PVs."""

import logging
import re
import time

import numpy as np
from p4p.server.raw import Handler

from p4p.nt import NTNDArray

from kafka_dae_diagnostics.data import Data
from p4p.server.thread import SharedPV

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
        self._channel_regex = re.compile(rf"^{re.escape(prefix)}SPEC:(\d+):(\d+):Y$")

    def testChannel(self, name: str) -> bool | str:
        """
        Test whether a channel with the given name can be served by this handler.

        Args:
            name: Channel name.
        """
        match = self._channel_regex.fullmatch(name)
        return match is not None \
            and int(match.group(1)) < self._data.spectra.shape[0] \
            and int(match.group(2)) < self._data.spectra.shape[1]

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

        data = self._data

        class ConnectionHandler:
            def onLastDisconnect(self, pv):
                logger.info(f"Closing channel {name} {peer}")
                data.spectrum_updaters.remove((period, det, pv))

        pv = SharedPV(
            nt=NTNDArray(),
            initial=data.spectra[period, det].astype(np.double),
            handler=ConnectionHandler(),
            timestamp=time.time(),
        )

        data.spectrum_updaters.append((period, det, pv))
        return pv
