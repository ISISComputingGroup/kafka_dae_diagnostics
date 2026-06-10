"""Veto diagnostics utilities."""

import threading
from collections import deque
from dataclasses import dataclass, field

import numpy as np
import numpy.typing as npt

NUM_VETOS: int = 32  # Vetos are transmitted as a u32


@dataclass(eq=False)
class VetoDiagnostics:
    """Veto diagnostics."""

    _run_veto_counts: npt.NDArray[np.int64] = field(
        default_factory=lambda: np.zeros(shape=(NUM_VETOS,), dtype=np.int64)
    )
    """
    Array containing the number of frames for which each veto was active, for
    frames in the current run.
    """

    _recent_veto_counts: npt.NDArray[np.int64] = field(
        default_factory=lambda: np.zeros(shape=(NUM_VETOS,), dtype=np.int64)
    )
    """
    Array containing the number of frames for which each veto was active, for
    recent frames (those still in the _recent_veto_masks queue).
    """

    _num_frames: int = 0
    """The total number of frames since the last call to :py:obj:`reset()`"""

    _recent_veto_masks: deque[int] = field(default_factory=deque)
    """
    Queue of the veto masks from the most recently received frames in this run.
    """

    _lock: threading.RLock = field(default_factory=threading.RLock)
    """Lock-object"""

    max_recent_frames: int = 100
    """Maximum number of frames to keep in the 'recent frames' queue."""

    def add_veto(self, veto: int) -> None:
        """Add a veto from a new frame."""
        with self._lock:
            self._recent_veto_masks.append(veto)

            for shift in range(NUM_VETOS):
                if (veto & (1 << shift)) != 0:
                    self._run_veto_counts[shift] += 1
                    self._recent_veto_counts[shift] += 1

            while len(self._recent_veto_masks) > self.max_recent_frames:
                # Decrement veto counters for frames which are no longer 'recent'
                evicted_vetos = self._recent_veto_masks.popleft()

                for shift in range(NUM_VETOS):
                    if (evicted_vetos & (1 << shift)) != 0:
                        self._recent_veto_counts[shift] -= 1

            self._num_frames += 1

    def reset(self) -> None:
        """Reset veto statistics (at the start of a new run)."""
        with self._lock:
            self._recent_veto_masks.clear()
            self._num_frames = 0
            self._run_veto_counts[:] = 0
            self._recent_veto_counts[:] = 0

    def get_run_veto_count(self) -> npt.NDArray[np.int64]:
        """Get an array of veto counts, keyed by veto index, for the whole run."""
        return self._run_veto_counts

    def get_recent_veto_count(self) -> npt.NDArray[np.int64]:
        """Get an array of veto counts, keyed by veto index, for recent frames."""
        return self._recent_veto_counts

    def get_run_veto_percentages(self) -> npt.NDArray[np.float64]:
        """Get an array of veto percentages, keyed by veto index, for the whole run."""
        with self._lock:
            num_frames = self._num_frames
            if num_frames == 0:
                return np.zeros((NUM_VETOS,), dtype=np.float64)
            else:
                return (self._run_veto_counts * 100.0) / num_frames

    def get_recent_veto_percentages(self) -> npt.NDArray[np.float64]:
        """Get an array of veto percentages, keyed by veto index, for recent frames."""
        with self._lock:
            num_frames = len(self._recent_veto_masks)
            if num_frames == 0:
                return np.zeros((NUM_VETOS,), dtype=np.float64)
            else:
                return (self._recent_veto_counts * 100.0) / num_frames
