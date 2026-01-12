"""Kafka DAE diagnostics."""

import logging
import time

import numpy as np
from p4p.nt import NTScalar
from p4p.server import DynamicProvider, Server
from p4p.server.thread import SharedPV

from kafka_dae_diagnostics._kdaediag_rs import (
    bin_events_into_spectrum,
    bin_events_into_spectrum_linear,
)

logging.basicConfig(level=logging.DEBUG)


class SpectrumPV(SharedPV):
    pass


spectrum_updaters = {}


class SpectrumHandler:
    def __init__(self) -> None:
        pass

    def testChannel(self, name: str) -> bool:
        print(f"Testing channel {name}")
        return name.startswith("TE:NDW2922:KDAEDIAG:")

    def makeChannel(self, name: str, peer: str) -> SharedPV:
        print(f"Making channel {name} {peer}")

        pv = spectrum_updaters.get(name)
        if pv is None:
            pv = SharedPV(nt=NTScalar("d"), initial=123)
            spectrum_updaters[name] = pv
        return pv


RNG = np.random.default_rng()


def bin_events_into_spectrum_np(
    histogram: npt.NDArray,
    event_tofs,
    pixel_ids,
    tof_bin_boundaries,
):
    indices = np.searchsorted(tof_bin_boundaries, event_tofs, side="right") - 1
    valid = np.logical_and(indices >= 0, indices < len(tof_bin_boundaries) - 1)
    np.add.at(histogram, (pixel_ids[valid], indices[valid]), 1)


def bin_events_into_spectrum_linear_np(
    histogram,
    event_tofs,
    pixel_ids,
    tof_bin_start,
    tof_bin_stop,
    tof_bin_step,
):
    indices = (event_tofs - tof_bin_start) // tof_bin_step
    valid = np.logical_and(indices >= 0, indices < (tof_bin_stop - tof_bin_start) / tof_bin_step)
    np.add.at(histogram, (pixel_ids[valid], indices[valid]), 1)


def main() -> None:
    detectors = 1000
    time_channels = 1000
    events_per_frame = 10_000

    arr = np.zeros((detectors, time_channels), dtype=np.uint64)
    arr2 = np.zeros((detectors, time_channels), dtype=np.uint64)
    arr3 = np.zeros((detectors, time_channels), dtype=np.uint64)
    arr4 = np.zeros((detectors, time_channels), dtype=np.uint64)

    detector_ids = RNG.integers(low=0, high=detectors, size=events_per_frame).astype(np.uint32)
    tofs = RNG.integers(low=0, high=20_000_000, size=events_per_frame).astype(np.uint32)

    boundaries = np.linspace(5_000_000, 15_000_000, num=time_channels + 1).astype(np.uint32)

    start = time.time()
    for _ in range(50_000):
        bin_events_into_spectrum(
            histogram=arr,
            event_tofs=tofs,
            pixel_ids=detector_ids,
            tof_bin_boundaries=boundaries,
        )
    end = time.time()

    print(f"rust/arbitrary bins sum = {np.sum(arr)}")
    print(f"{(end - start) * 1000} ms")

    start = time.time()
    for _ in range(50_000):
        bin_events_into_spectrum_np(
            histogram=arr2,
            event_tofs=tofs,
            pixel_ids=detector_ids,
            tof_bin_boundaries=boundaries,
        )
    end = time.time()

    print(f"numpy/arbitrary bins sum = {np.sum(arr2)}")
    print(f"{(end - start) * 1000} ms")

    start = time.time()
    for _ in range(50_000):
        bin_events_into_spectrum_linear(
            histogram=arr3,
            event_tofs=tofs,
            pixel_ids=detector_ids,
            tof_bin_start=5_000_000,
            tof_bin_stop=15_000_000,
            tof_bin_step=10000,
        )
    end = time.time()

    print(f"rust/linear bins sum = {np.sum(arr3)}")
    print(f"{(end - start) * 1000} ms")

    start = time.time()
    for _ in range(50_000):
        bin_events_into_spectrum_linear_np(
            histogram=arr4,
            event_tofs=tofs,
            pixel_ids=detector_ids,
            tof_bin_start=5_000_000,
            tof_bin_stop=15_000_000,
            tof_bin_step=10000,
        )
    end = time.time()

    print(f"numpy/linear bins sum = {np.sum(arr4)}")
    print(f"{(end - start) * 1000} ms")

    np.testing.assert_array_equal(arr, arr2)
    np.testing.assert_array_equal(arr2, arr3)
    np.testing.assert_array_equal(arr3, arr4)
    return

    handler = SpectrumHandler()
    providers = [
        DynamicProvider("spectra", handler=handler),
    ]
    server = Server(providers=providers)

    with server:
        while True:
            time.sleep(1)
            print(spectrum_updaters)
