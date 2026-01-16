"""Data being served by this IOC."""

import dataclasses
import threading
import time
from datetime import date
from typing import Callable

import numpy.typing as npt
import numpy as np

from p4p.server.thread import SharedPV


@dataclasses.dataclass
class Data:
    """A mutable object describing the data being served."""

    spectra: npt.NDArray[np.uint]
    """
    An array describing counts since last run start.

    Has shape ``(n_periods, n_detectors, n_timechannels)`` and
    data-type :py:obj:`numpy.uint`.
    """

    callbacks: dict[str, Callable[["Data"], None]]
    """
    A list of callbacks to notify when data is updated.
    """

    bin_boundaries: npt.NDArray[np.int32]
    """
    Time-bin boundaries (ns).
    """

    callbacks_lock: threading.RLock = threading.RLock()
    """
    Lock-object, must be taken when spectrum_updaters is iterated/mutated
    :meta private:
    """

    total_events: int = 0
    """
    Total number of neutron events in this run.
    """

    total_event_messages: int = 0
    """
    Total number of ev44 event messages in this run.
    """

    total_event_megabytes: float = 0.
    """
    Megabytes of event messages processed in this run.
    """

    largest_kafka_timestamp: float = 0.
    """
    Largest timestamp seen in an ev44 or pl72 message since
    the beginning of this run. Seconds since epoch.
    """

    most_recent_kafka_timestamp: float = 0.
    """
    Timestamp in the most recently-processed ev44 or pl72 message.
    Seconds since epoch.
    """

    start_time: float = 0.
    """
    Timestamp of the most recent pl72 run-start message.
    Seconds since epoch.
    """

    processing_lag: float = 0.
    """
    Estimated time difference between an event being recorded in
    electronics and processed in KDAEDIAG ioc.
    """

    @property
    def mev(self):
        return self.total_events / 1_000_000

    @property
    def duration(self):
        return max(self.largest_kafka_timestamp - self.start_time, 0)

    @property
    def mev_per_hour(self):
        duration = self.duration
        if duration == 0:
            return 0
        return (self.mev / duration) * 3600

    @property
    def num_periods(self) -> int:
        return self.spectra.shape[0]

    @property
    def num_detectors(self) -> int:
        return self.spectra.shape[1]

    @property
    def num_time_channels(self) -> int:
        return self.spectra.shape[2]

    @property
    def histogram_megabytes(self) -> int:
        return self.spectra.nbytes / 1024**2
