# 2. Rust extension module

## Status

Current

## Context

This module will need to deliver a range of DAE diagnostics information, including
time-of-flight spectra for each period.

Binning incoming events into spectra as they arrive can be performance-sensitive, dependent
on event rate and numbers of periods/spectra/time channels.

:::{note}
The updates will not actually be _sent_ to EPICS PVs unless a subscriber is attached; see
{doc}`001_server_structure`. However, a spectrum array needs to be held and regularly updated
in memory in this process, to allow for fast reads at the point when a client reads a spectrum.
:::

Options considered are:
- Implement histogramming in Python (with {py:obj}`numpy`)
  - {py:obj}`numpy.searchsorted` on each event to find the appropriate bin.
- Implement histogramming in native extension (with `PyO3`)
  - Based on [a similar algorithm in Mantid](https://github.com/mantidproject/mantid/blob/0d4de52da17df6d055e285902e87ebc53d2de7f6/Framework/DataObjects/src/EventList.cpp#L2575)

Both approaches were benchmarked on time taken to histogram the following test data (with a reasonably
representative set of parameters):
- {math}`50000` frames, each containing {math}`10000` events
- Detector IDs randomly distributed between {math}`0` and {math}`50000`
- Time-of-flights following Gaussian distribution around {math}`10000000` μs with {math}`σ = 2000000` μs
- Binning into {math}`1000` evenly-spaced bins between {math}`5000000` and {math}`15000000` ns

| Implementation | Time (s) | Throughput (`Gbit/s`) | Throughput (`Mevents/s`) |
|----------------|----------|-----------------------|--------------------------|
| `numpy`        | 85.7     | 2.8                   | 5.8                      |
| native (PyO3)  | 5.8      | 41.7                  | 85.5                     |

For an estimated representative set of parameters for HRPD-X:
- {math}`4000` frames (i.e. 100 seconds at 40Hz), each containing {math}`26000` events
- Detector IDs randomly distributed between {math}`0` and {math}`16000`
- Time-of-flights following Gaussian distribution around {math}`10000000` μs with {math}`σ = 2000000` μs
- Binning into {math}`8000` evenly-spaced bins between {math}`5000000` and {math}`15000000` ns

| Implementation | Time (s) | Throughput (`Gbit/s`) | Throughput (`Mevents/s`) |
|----------------|----------|-----------------------|--------------------------|
| `numpy`        | 19.4     | 2.6                   | 5.3                      |
| native (PyO3)  | 1.5      | 33.1                  | 67.6                     |

An analysis of count rates across all _existing_ instruments was done for MNeuData; many existing
ISIS instruments {abbr}`regularly (99th percentile of runs recorded in journal)` have count rates
between 1-5 MEvents/s, with higher peak count rates within a run and in exceptional setups.

Discussion with DSG suggests that their side of the streaming setup (for example UDP to Kafka) has maximum
throughput of around 8 Gbit/s *per WLSF module* - though this strongly depends on hardware specifications.
Instruments will have _many_ WLSF modules (for example, HRPD-X is expected to have 80 modules).
HRPD-X is expected to have multiple monitors counting at ~100s kHz, and a detector flux around 3x higher than
HRPD due to WLSF detector efficiency upgrades.

## Decision

Implement histogramming using a native PyO3 extension.

Although this makes this project slightly more complicated to develop and deploy, the performance gains
seem to be large enough in this case to justify the moderate increase in complexity.

## Consequences

- This module will be primarily Python, with a Rust native extension library used to implement
performance-sensitive operations
- The code will be slightly more difficult to build than a pure-python library.
  - `maturin` + `PyO3` make this relatively easy, but it is still _more_ difficult than pure-python.
- Developers will need some awareness of Rust to modify the native extension.
- Histogramming spectra will use fewer system resources compared to a {py:obj}`numpy` implementation. This aligns
with ISIS computing sustainability/energy reduction goals.
