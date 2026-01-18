import numpy as np
import numpy.typing as npt

def bin_events_into_spectrum(
    histogram: npt.NDArray[np.float64],
    event_tofs: npt.NDArray[np.int32],
    pixel_ids: npt.NDArray[np.int32],
    tof_bin_boundaries: npt.NDArray[np.int32],
) -> None:
    pass

def bin_events_into_spectrum_linear(
    histogram: npt.NDArray[np.float64],
    event_tofs: npt.NDArray[np.int32],
    pixel_ids: npt.NDArray[np.int32],
    tof_bin_start: int,
    tof_bin_stop: int,
    tof_bin_step: int,
) -> None:
    pass
