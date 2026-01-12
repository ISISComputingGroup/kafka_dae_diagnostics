# 2. Rust extension module

## Status

Current

## Context

This module will need to deliver a range of DAE diagnostics information, including
time-of-flight spectra for each period.

Binning incoming events into spectra as they arrive could be performance-sensitive (dependent
on event rate).

:::{note}
The updates will not actually be _sent_ to EPICS PVs unless a subscriber is attached; see
{doc}`001_server_structure`. However, a spectrum array needs to be held and regularly updated
in memory in this process, to allow for fast reads at the point when a client reads a spectrum.
:::

Options considered are:
- Implement histogramming in Python (with {py:obj}`numpy`)
- Implement histogramming in native extension (with `PyO3`)

Both approaches were benchmarked on time taken to histogram the following test data (with a reasonably
representative set of parameters):
- {math}`50000` frames, each containing {math}`10000` events
- Detector IDs randomly distributed between {math}`0` and {math}`50000`
- Time-of-flights randomly distributed between {math}`0` and {math}`20000000` ns
- Binning into {math}`1000` evenly-spaced bins between {math}`5000000` and {math}`15000000` ns

For arbitrary bins, a binary search is used to find the target bin for an event. This is {math}`\small O(log(N))`.
For numpy, the implementation is {py:obj}`numpy.searchsorted`.
For linear bins, an {math}`\small O(1)` bin lookup is used.
The implementations were verified to give identical results.

Benchmark results:
- **Native extension**, arbitrary bins: 10.4 seconds
  * Histogramming 47.9 MEvents/s
  * ~3.1 Gbit/s of `ev44`)
- {py:obj}`numpy`, arbitrary bins: 33.6 seconds
  - Histogramming 14.9 MEvents/s
  - ~950 Mbit/s of `ev44`
- **Native extension**, linear bins: 1.7 seconds
  * Histogramming 294 MEvents/s
  * ~19 Gbit/s of `ev44`)
- {py:obj}`numpy`, linear bins: 16.4 seconds
  - Histogramming 30.5 MEvents/s
  - ~2 Gbit/s of `ev44`

An analysis of count rates across all _existing_ instruments was done for MNeuData; many existing
ISIS instruments {abbr}`regularly (99th percentile of runs recorded in journal)` have count rates
between 1-5 MEvents/s, with higher peak count rates within a run and in exceptional setups.

HRPD-X is expected to have multiple monitors counting at ~100s KHz, and a detector flux around 3x higher than
HRPD due to WLSF detector efficiency upgrades.

5 MEvents/s corresponds to approximately 320 Mbit/s of `ev44` messages.

Discussion with DSG suggests that their side of the streaming setup (for example UDP to Kafka) has maximum
throughput of around 8 Gbit/s per WLSF module - though this strongly depends on hardware specifications.

## Decision

Implement histogramming using a native PyO3 extension.

Although this makes this project slightly more complicated to develop and deploy, the performance gains
seem to be large enough in this case to justify the moderate increase in complexity.

Writing the histogramming in native code allows more 'obvious' code to be written - while it is _reasonably_
performant, the vectorized numpy code is not an obvious implementation and will be more difficult
to extend in a performant way than equivalent native code, especially when adding filtering such as vetoes.

## Consequences

- This module will be primarily Python, with a Rust native extension library where performance is a
concern (for example, histogramming spectra).
- The code will be slightly more difficult to build than a pure-python library.
  - `maturin` + `PyO3` make this relatively easy, but it is still _more_ difficult than pure-python.
- Developers will need some awareness of Rust to modify the native extension.
- Histogramming spectra will use fewer system resources compared to a {py:obj}`numpy` implementation. This aligns
with ISIS computing sustainability/energy reduction goals.
