# 2. Approach to histogramming spectra

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

In both approaches, a binary search algorithm is used to select the target bin. In the numpy case,
this is {py:obj}`numpy.searchsorted`, in the native extension case it is a manually written binary
search. Faster {math}`\small O(1)` algorithms are available if bins are _known_ to be linearly spaced in
advance; either approach could be adjusted with this optimization, so it is ignored for benchmarking.

The implementations were verified to give identical results.

Benchmark results:
- **Native extension**: 10.4 seconds
  * Histogramming 47.9 MEvents/s
  * Approximately 3.1 Gbit/s of `ev44`)
- {py:obj}`numpy`: 33.6 seconds
  - Histogramming 14.9 MEvents/s
  - Approximately 950 Mbit/s of `ev44`

An analysis of count rates across all _existing_ instruments was done for MNeuData; many existing
ISIS instruments {abbr}`regularly (99th percentile of runs recorded in journal)` have count rates
between 1-5 MEvents/s, with higher peak count rates within a run and in exceptional setups.

5 MEvents/s corresponds to approximately 320 Mbit/s of `ev44` messages.

## Decision

- Initially, implement histogramming using {py:obj}`numpy` as it is _reasonably_ performant and keeps this
module pure-python.
- If we later see inadequate performance using the {py:obj}`numpy` approach, we may reimplement histogramming
using a PyO3 native extension to get a ~3x performance increase.

## Consequences

- Some performance will be lost, relative to a native extension
- `kafka_dae_diagnostics` will remain a pure-python project for now
- We may revisit this decision in future if performance becomes an issue
