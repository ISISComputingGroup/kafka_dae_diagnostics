"""Data being served by this IOC."""

import dataclasses

import numpy.typing as npt
import numpy as np

from p4p.server.thread import SharedPV


@dataclasses.dataclass
class Data:
    """A mutable object describing the data being served."""

    spectra: npt.NDArray[np.uint64]
    """
    An array describing counts since last run start.

    Has shape ``(n_periods, n_detectors, n_timechannels)`` and
    data-type :py:obj:`numpy.uint64`.
    """

    spectrum_updaters: list[tuple[int, int, SharedPV]]
    """
    A list of callbacks to notify when a spectrum is updated.

    Arguments are (period, detector, :py:obj:`~p4p.server.thread.SharedPV`).
    """
