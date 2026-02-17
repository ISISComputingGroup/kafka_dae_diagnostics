"""Benchmark scripts for ev44 message processing."""

import time

import numpy as np
from streaming_data_types import serialise_ev44

from kafka_dae_diagnostics.data import Data
from kafka_dae_diagnostics.kafka.handlers import handle_ev44

RNG = np.random.default_rng(seed=0)


def generate_fake_events(  # noqa: PLR0913 PLR0917 (benchmark script only)
    msg_id: int,
    events_per_frame: int,
    tof_peak: float,
    tof_sigma: float,
    det_min: int,
    det_max: int,
    sorted: bool,
) -> bytes:
    """Generate fake flatbuffers-encoded ev44 messages."""
    detector_ids = RNG.integers(low=det_min, high=det_max, size=events_per_frame)
    tofs = np.maximum(0.0, RNG.normal(loc=tof_peak, scale=tof_sigma, size=events_per_frame))
    if sorted:
        tofs.sort()

    return serialise_ev44(
        source_name="saluki",
        reference_time=[time.time() * 1_000_000_000],
        message_id=msg_id,
        reference_time_index=[0],
        time_of_flight=tofs,
        pixel_id=detector_ids,
    )


def benchmark_ev44_processing(
    n_ev44: int, n_events: int, n_bins: int, n_detectors: int, sorted: bool
) -> None:
    """Run an ev44 processing benchmark."""
    data = Data(
        spectra=np.zeros((1, n_detectors, n_bins), dtype=np.float64),
        bin_boundaries=np.linspace(5_000_000, 15_000_000, n_bins + 1, dtype=np.int32),
    )
    msgs = [
        generate_fake_events(0, n_events, 10_000_000, 2_000_000, 0, n_detectors, sorted=sorted)
        for _ in range(n_ev44)
    ]
    len_bytes = sum(len(msg) for msg in msgs)

    start = time.time()
    for msg in msgs:
        handle_ev44(data, msg)
    end = time.time()
    t = end - start

    mib_per_sec = len_bytes * 8 / (1024 * 1024 * t)

    print(
        f"{n_ev44} ev44 messages, "
        f"{n_events} ev/msg, "
        f"{n_bins} bins, "
        f"{n_detectors} detectors, "
        f"ev44 {'sorted' if sorted else 'unsorted'}"
    )
    print(f"{t * 1000:.3f} ms ({t * 1000 / len(msgs):.6f} ms/msg)")
    print(f"{mib_per_sec:.3f} MiB/s ({mib_per_sec * 8:.3f} Mbit/s)")
    print(f"{n_events * n_ev44 / (1_000_000 * t):.3f} Mev/s\n")


if __name__ == "__main__":
    # 1s worth of data in 'expected' shape for HRPD-X, at different count rates.
    # HRPD-x is expected to have between 80 and 400 pixels per FPGA, and 80 FPGAs
    # Each FPGA will send one ev44 per frame - at 40Hz that is 3200 frames/sec.
    #
    # HRPD collected ~2600 events per frame (99th percentile of completed runs).
    # HRPD-x expects to have 6-10x HRPD count rate so ~26000 events per frame or
    # 325 events per frame per detector ev44.
    #
    # Benchmark histogramming into 8000 time bins, with expected HRPD-X count rate and with
    # 10x the above count rate to simulate an exceptionally 'hot' run.
    #
    # If these benchmarks take less than one second, histogramming will be able to keep
    # up with HRPD-x sustained data rate.
    #
    # HRPD-X's ev44 messages are expected to be emitted in ToF order.
    benchmark_ev44_processing(3200, 325, 8000, 16000, True)

    # These benchmarks are representative after a 'grouping' process which batches events
    # from multiple detectors so we get one ev44 per frame. Same number of events as above.
    # Always sorted as batching process can sort by ToF.
    benchmark_ev44_processing(40, 26000, 8000, 16000, True)
