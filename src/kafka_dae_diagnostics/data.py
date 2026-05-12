"""Data being served by this IOC."""

import dataclasses
import threading
from collections.abc import Callable
from dataclasses import field

import numpy as np
import numpy.typing as npt


@dataclasses.dataclass
class FrameMetaData:
    """Metadata associated with a set of neutron events."""

    vetoes: int = 0
    """Integer mask of active vetoes in this frame."""

    proton_charge: float = 0.0
    """Proton charge, in uAh per frame"""

    period: int = 0
    """Period into which this data was collected"""


@dataclasses.dataclass
class Data:
    """A mutable object describing the data being served by this IOC."""

    spectra: npt.NDArray[np.float64] = field(
        default_factory=lambda: np.zeros(shape=(1, 1, 1), dtype=np.float64)
    )
    """
    An array describing counts since last run start.

    Has shape ``(n_periods, n_detectors, n_timechannels)``.
    """

    callbacks: dict[str, Callable[[], None]] = field(default_factory=dict)
    """
    A list of callbacks to notify when data is updated.
    """

    bin_boundaries: npt.NDArray[np.int32] = field(
        default_factory=lambda: np.linspace(0, 100_000_000, num=2, dtype=np.int32)
    )
    """
    Time-bin boundaries (ns).
    """

    callbacks_lock: threading.RLock = field(default_factory=threading.RLock)
    """
    Lock-object, must be taken when ``spectrum_updaters`` is iterated/mutated.
    :meta private:
    """

    total_events: int = 0
    """
    Total number of good neutron events in this run.
    """

    total_event_messages: int = 0
    """
    Total number of ev44 event messages in this run.
    """

    total_event_megabytes: float = 0.0
    """
    Megabytes of event messages processed in this run.
    """

    largest_kafka_timestamp: float = 0.0
    """
    Largest timestamp seen in an ``ev44``, ``pu00`` or ``pl72`` message since
    the beginning of this run. Seconds since epoch.
    """

    most_recent_kafka_timestamp: float = 0.0
    """
    Timestamp in the most recently-processed ``ev44``, ``pu00`` or ``pl72`` message.
    Seconds since epoch.
    """

    start_time: float = 0.0
    """
    Timestamp of the most recent ``pl72`` run-start message.
    Seconds since epoch.
    """

    stop_time: float = 0.0
    """
    Timestamp of the most recent 6s4t run-stop message.
    Seconds since epoch.
    """

    event_processing_lag: float = 0.0
    """
    Estimated time difference between an event being recorded in
    electronics and processed in KDAEDIAG IOC.
    """

    frame_metadata: dict[int, FrameMetaData] = field(default_factory=dict)
    """
    Metadata for the current frame, keyed by Kafka partition ID.
    """

    raw_frames_pd: npt.NDArray[np.int64] = field(
        default_factory=lambda: np.zeros(shape=(1,), dtype=np.int64)
    )
    """
    Array of raw frames collected in each period.
    """

    good_frames_pd: npt.NDArray[np.int64] = field(
        default_factory=lambda: np.zeros(shape=(1,), dtype=np.int64)
    )
    """
    Array of good frames collected in each period.
    """

    raw_frames: int = 0
    """
    Number of raw frames seen in the current run.
    """

    good_frames: int = 0
    """
    Number of good (non-vetoed) frames seen in the current run.
    """

    raw_uah_pd: npt.NDArray[np.float64] = field(
        default_factory=lambda: np.zeros(shape=(1,), dtype=np.float64)
    )
    """
    Array of raw uAh collected in each period.
    """

    good_uah_pd: npt.NDArray[np.float64] = field(
        default_factory=lambda: np.zeros(shape=(1,), dtype=np.float64)
    )
    """
    Array of good uAh collected in each period.
    """

    raw_uah: float = 0.0
    """
    Raw uAh collected in the current run (including vetoed frames).
    """

    good_uah: float = 0.0
    """
    Good uAh collected in the current run.
    """

    veto_mask: int = 0
    """
    Integer mask of enabled vetoes.
    """

    @property
    def mev(self) -> float:
        """Number of counts (in millions of events)."""
        return self.total_events / 1_000_000

    @property
    def duration(self) -> float:
        """Run duration in seconds."""
        return max(self.largest_kafka_timestamp - self.start_time, 0)

    @property
    def mev_per_hour(self) -> float:
        """Number of counts per hour (in millions of events)."""
        duration = self.duration
        if duration == 0:
            return 0
        return (self.mev / duration) * 3600

    @property
    def num_periods(self) -> int:
        """Number of periods in histogram."""
        return self.spectra.shape[0]

    @property
    def num_spectra(self) -> int:
        """Number of spectra in histogram."""
        return self.spectra.shape[1]

    @property
    def num_time_channels(self) -> int:
        """Number of time channels in histogram."""
        return self.spectra.shape[2]

    @property
    def histogram_megabytes(self) -> float:
        """Size of histogram array in MiB."""
        return self.spectra.nbytes / 1024**2

    @property
    def average_data_rate(self) -> float:
        """Average data rate of this run in MiB/s."""
        duration = self.duration
        if duration == 0:
            return 0
        return self.total_event_megabytes / duration

    @property
    def count_rate(self) -> float:
        """Average count rate during this run in MEv/h.

        Includes good counts only.
        """
        duration = self.duration
        if duration == 0:
            return 0
        return (self.total_events * 3600) / (duration * 1_000_000)
