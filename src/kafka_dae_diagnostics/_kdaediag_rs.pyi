import numpy.typing as npt

def bin_events_into_spectrum(
    histogram: npt.NDArray,
    event_tofs: npt.NDArray,
    pixel_ids: npt.NDArray,
    tof_bin_boundaries: npt.NDArray,
) -> npt.NDArray:
    pass


def bin_events_into_spectrum_linear(
    histogram: npt.NDArray,
    event_tofs: npt.NDArray,
    pixel_ids: npt.NDArray,
    tof_bin_start: int,
    tof_bin_stop: int,
    tof_bin_step: int,
) -> npt.NDArray:
    pass
