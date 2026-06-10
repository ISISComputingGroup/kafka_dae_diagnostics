""":py:obj:`p4p.server.raw.Handler` types for spectrum PVs."""

import logging
import re
import uuid
from typing import Any

import numpy as np
import numpy.typing as npt
from p4p.nt import NTScalar
from p4p.server.raw import Handler
from p4p.server.thread import SharedPV

from kafka_dae_diagnostics._kdaediag_rs import Data

logger = logging.getLogger(__name__)


class SpectrumHandler(Handler):
    """Handle Spectrum Y (counts) data."""

    def __init__(self, prefix: str, data: Data) -> None:
        """Handle Spectrum Y (counts) data.

        Args:
            prefix: PV prefix (e.g. ``IN:INSTNAME:KDAEDIAG:``)
            data: Reference to data being served.

        """
        self._data = data
        self._prefix = prefix
        self._channel_regex = re.compile(rf"^{re.escape(prefix)}SPEC:(\d+):(\d+):([XY])$")

    def testChannel(self, name: str) -> bool | str:
        """Test whether a channel with the given name can be served by this handler.

        Args:
            name: Channel name.

        """
        match = self._channel_regex.fullmatch(name)
        return (
            match is not None
            and int(match.group(1)) < self._data.num_periods
            and int(match.group(2)) < self._data.num_spectra
        )

    def makeChannel(self, name: str, peer: str) -> SharedPV:
        """Make a channel for the PV with the given name.

        Args:
            name: Channel name.
            peer: remote address.

        """
        logger.info("Opening channel %s %s", name, peer)

        match = self._channel_regex.fullmatch(name)
        assert match is not None, "No match in makeChannel after there was a match in testChannel?"
        period = int(match.group(1))
        det = int(match.group(2))
        typ = match.group(3)

        callback_id = f"{name}#{uuid.uuid4()}"

        def extract_data(
            typ: str = typ, period: int = period, det: int = det
        ) -> npt.NDArray[np.float64]:
            match typ:
                case "Y":
                    return self._data.spectra[period][det]
                case "X":
                    return (
                        (self._data.bin_boundaries[1:] + self._data.bin_boundaries[:-1]) / 2
                    ).astype(np.float64)
                case _:  # pragma: no cover (unreachable)
                    raise ValueError(f"Unknown channel type: {typ}")

        class ConnectionHandler:
            @staticmethod
            def onLastDisconnect(*_: list[Any], **__: dict[str, Any]) -> None:  # noqa N802 p4p requires this name
                logger.info("Closing channel %s %s", name, peer)
                with self._data.callbacks_lock:
                    del self._data.callbacks[callback_id]

        pv = SharedPV(
            nt=NTScalar("ad"),
            initial=extract_data(),
            handler=ConnectionHandler(),
        )

        with self._data.callbacks_lock:
            self._data.callbacks[callback_id] = lambda: pv.post(extract_data())

        return pv
