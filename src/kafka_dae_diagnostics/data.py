"""Data being served by this IOC."""

import dataclasses
import enum
import threading
import time
from collections.abc import Callable
from dataclasses import field
from datetime import datetime
from zoneinfo import ZoneInfo

import numpy as np
import numpy.typing as npt

from kafka_dae_diagnostics.veto_diagnostics import NUM_VETOS, VetoDiagnostics

TIMEZONE = ZoneInfo("Europe/London")


class RunState(enum.IntEnum):
    """Enum describing the possible run-states."""

    PROCESSING = 0
    SETUP = 1
    RUNNING = 2
    PAUSED = 3
    WAITING = 4
    VETOING = 5


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

    stale_event_timeout_s: float = 5.0
    """
    The time interval (seconds) in which we call the system 'PROCESSING' if we have not received
    new event messages.
    """

    vetoing_percentage: float = 50.0
    """The minimum percentage at which the DAE is considered 'vetoing' rather than 'running'."""

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
    Active time-bin boundaries for the current run (ns).
    """

    binning_start_ns: int = 0
    """
    Binning start (nanoseconds), takes effect on next run start.
    """

    @property
    def binning_start_us(self) -> float:
        """Binning start (microseconds), takes effect on next run start."""
        return self.binning_start_ns / 1000.0

    @binning_start_us.setter
    def binning_start_us(self, value: float) -> None:
        self.binning_start_ns = round(value * 1000.0)

    binning_end_ns: int = 100_000_000
    """
    Binning end (nanoseconds), takes effect on next run start.
    """

    @property
    def binning_end_us(self) -> float:
        """Binning end (microseconds), takes effect on next run start."""
        return self.binning_end_ns / 1000.0

    @binning_end_us.setter
    def binning_end_us(self, value: float) -> None:
        self.binning_end_ns = round(value * 1000.0)

    binning_num_points: int = 500
    """
    Number of time-bins to generate between `binning_start_ns` and `binning_end_ns`.
    Takes effect on next run start.
    """

    callbacks_lock: threading.RLock = field(default_factory=threading.RLock)
    """
    Lock-object, must be taken when ``spectrum_updaters`` is iterated/mutated.
    :meta private:
    """

    total_events: int = 0
    """
    Total number of non-vetoed neutron events in this run.
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

    most_recent_event_processing_timestamp: float = field(default_factory=time.time)
    """
    The computer timestamp at which the most recent event message was processed by this IOC.
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

    frame_metadata: dict[int | None, FrameMetaData] = field(default_factory=dict)
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

    veto_mask: int = 0xFFFF
    """
    Integer mask of enabled vetoes.
    """

    veto_diagnostics: VetoDiagnostics = field(default_factory=VetoDiagnostics)
    """Veto diagnostics."""

    veto_names_array: npt.NDArray[np.str_] = field(
        default_factory=lambda: np.array([f"veto_{n}" for n in range(NUM_VETOS)], dtype=np.str_)
    )
    """
    Veto names, as a numpy array of strings.
    """

    current_period: int = 0
    """
    The zero-indexed period into which the most recent data was received.
    """

    @property
    def enabled_vetos_array(self) -> npt.NDArray[np.int32]:
        """Array describing whether each of the 32 veto bits is currently enabled."""
        return ((self.veto_mask & (1 << np.arange(NUM_VETOS))) != 0).astype(np.int32)

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

    @property
    def seconds_since_last_event_message(self) -> float:
        """The number of seconds elapsed since the last event message was processed by this IOC."""
        return max(time.time() - self.most_recent_event_processing_timestamp, 0.0)

    @property
    def run_state(self) -> RunState:
        """The current run state.

        - **SETUP**: the electronics is currently idle, not performing a run.
        - **PROCESSING**: the electronics should be running, but we have not received
            event messages from Kafka recently. This is a fault condition.
        - **VETOING**: the electronics has sent us frames, but more than 50% of the recent
            frames have been vetoed.
        - **RUNNING**: the electronics has sent us frames, less than 50% of the recent
            frames have been vetoed.
        """
        if self.stop_time > self.start_time:
            # Most recent message is a run stop -> we're not running.
            return RunState.SETUP
        elif self.seconds_since_last_event_message > self.stale_event_timeout_s:
            # We think we should be RUNNING, but are not receiving
            # event messages, flag "PROCESSING" as this likely implies
            # that pipeline is 'stuck' at some level.
            return RunState.PROCESSING
        elif np.any(
            self.veto_diagnostics.get_recent_veto_percentages() >= self.vetoing_percentage,
            where=self.enabled_vetos_array.astype(np.bool_),
        ):
            # In principle this could fail to detect 100% vetoing if
            # e.g. veto1-4 veto 25% each (in a fully-non-overlapping way).
            # That is a somewhat pathological case...
            return RunState.VETOING
        else:
            return RunState.RUNNING

    @property
    def start_time_str(self) -> str:
        """The time of the most recent run-start message, formatted as a string."""
        if self.start_time > 0:
            return datetime.fromtimestamp(self.start_time, tz=TIMEZONE).strftime(
                "%a %d-%b-%Y %H:%M:%S"
            )
        else:
            return "N/A"

    @property
    def stop_time_str(self) -> str:
        """The time of the most recent run-stop message, formatted as a string."""
        if self.stop_time > 0:
            return datetime.fromtimestamp(self.stop_time, tz=TIMEZONE).strftime(
                "%a %d-%b-%Y %H:%M:%S"
            )
        else:
            return "N/A"
