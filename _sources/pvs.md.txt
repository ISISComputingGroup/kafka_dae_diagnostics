# PVs

This page describes the data held by each PV served by this IOC. See {py:class}`~kafka_dae_diagnostics.data.Data` for
the internal data model used by this IOC.

:::{dropdown} Run information

### `RUNSTATE`

The overall run state of the streaming system.

The value of this PV may be one of: PROCESSING, SETUP, RUNNING, PAUSED, WAITING, VETOING.

The logic used to decide the run state is:
- If latest `runInfo` message is a run stop: **SETUP**
- If latest `runInfo` message is a run start:
  - If we have not received 'recent' frames on `_event` topic for this run: **PROCESSING**
  - If percentage of vetoed frames in last {math}`N` received frames >=50%, **VETOING**, otherwise **RUNNING**

The PAUSED and WAITING run states are not currently implemented.

### `RUNSTATE_STR`

This PV is identical to the `RUNSTATE` PV, but as a string rather than an enumeration data type.

### `START_TIME` / `STOP_TIME`

The most recent run-start and run-stop timestamps (seconds since the UNIX epoch).

### `STARTTIME` / `STOPTIME`

The most recent run-start and run-stop times, formatted as strings.

### `RUNDURATION`

Duration, in seconds, of the current or most recent run.
:::

:::{dropdown} Frame readbacks
### `GOODFRAMES` / `RAWFRAMES`

Number of ISIS frames in the current run.

`GOODFRAMES` counts only non-vetoed frames, `RAWFRAMES` counts both vetoed and non-vetoed frames.

### `GOODFRAMES_PD` / `RAWFRAMES_PD`

Array of good/raw ISIS frames in the current run, keyed by period number. The array has length `NUMPERIODS`.

::::{note}
The sum of this array may differ from `GOODFRAMES` if frames with an invalid period number are received. Those frames will be included in `GOODFRAMES`, but will not appear in `GOODFRAMES_PD`.
::::
:::

:::{dropdown} μAh readbacks
### `GOODUAH` / `RAWUAH`

Accumulated micro-amp-hours in the current run.

`GOODUAH` counts only micro-amp-hours for the non-vetoed frames, `RAWUAH` counts both vetoed and non-vetoed frames.

### `GOODUAH_PD` / `RAWUAH_PD`

Array of good/raw micro-amp-hours in the current run, keyed by period number. The array has length `NUMPERIODS`.

::::{note}
The sum of `GOODUAH_PD` may differ from `GOODUAH` if frames with an invalid period number are received. Those frames will be included in `GOODUAH`, but will not appear in `GOODUAH_PD`.
::::
:::

:::{dropdown} Neutron event statistics
### `EVENTS` / `TOTALEVENTS`

The total number of non-vetoed neutron events seen during the current run.

`EVENTS` and `TOTALEVENTS` are aliases of each other, and provide the same information.

### `MEVENTS`

Millions of events seen during the current run. Equivalent to `EVENTS` divided by {math}`10^6`.

### `COUNTRATE`

The average count rate of the current run, in millions of events per hour.

This only includes *good* events.
:::

:::{dropdown} Vetos
### `VETO:RECENT:PERCENT`

The percentage of frames which were vetoed, within the most recently processed 100 frames in this run
(configurable via {py:obj}`~kafka_dae_diagnostics.veto_diagnostics.VetoDiagnostics.max_recent_frames`).

This PV is an array of length 32, where element `N` corresponds to a veto mask of `1 << N`.

### `VETO:RECENT:COUNT`

The number of frames which were vetoed, within the most recently processed 100 frames in this run
(configurable via {py:obj}`~kafka_dae_diagnostics.veto_diagnostics.VetoDiagnostics.max_recent_frames`).

This PV is an array of length 32, where element `N` corresponds to a veto mask of `1 << N`.

### `VETO:RUN:PERCENT`

The percentage of frames which were vetoed, within the current run.

This PV is an array of length 32, where element `N` corresponds to a veto mask of `1 << N`.

### `VETO:RUN:COUNT`

The number of frames which were vetoed, within the current run.

This PV is an array of length 32, where element `N` corresponds to a veto mask of `1 << N`.

### `VETO:ENABLED`

This PV is an array of length 32, where element `N` corresponds to a veto mask of `1 << N`. The value will be
`1` if the veto is enabled (either soft or hard), or `0` if the veto is disabled.

### `VETO:NAMES`

This PV is an array of length 32, where element `N` corresponds to a veto mask of `1 << N`. The values are the
user-friendly string names of each veto, for example `External veto 0`.
:::

:::{dropdown} Diagnostic histograms (spectra)

### `NUMPERIODS`

Number of periods configured in the current run. Valid period numbers are strictly less than this number.

### `NUMSPECTRA`

Number of spectra configured for the diagnostic histogram of the current run.

### `NUMTIMECHANNELS`

Number of time channels configured for the diagnostic histogram of the current run.

### `HISTMEMORY`

Memory used by diagnostic histograms, in `MiB`.

This is `num_periods * num_spectra * num_time_channels * 8 bytes`.

### `SPEC:<period>:<spectrum>:X`

These PVs provide the X points (the time-of-flight bin centres) for a time of flight spectrum. This array has length `NUMTIMECHANNELS`.

### `SPEC:<period>:<spectrum>:XE`

These PVs provide the X bin *edges* for a time of flight spectrum. This array has length `NUMTIMECHANNELS + 1`.

### `SPEC:<period>:<spectrum>:Y`

These PVs provide the normalised Y data (the number of counts in the relevant bin, divided by bin width) for a time of
flight spectrum. This array has length `NUMTIMECHANNELS`.

### `SPEC:<period>:<spectrum>:YC`

These PVs provide the raw Y data (the number of counts in the relevant bin) for a time of flight spectrum.
This array has length `NUMTIMECHANNELS`.

::::{important}
`<period>` is {external+ibex_developers_manual:doc}`one-indexed in all user-facing PVs <specific_iocs/datastreaming/ADRs/007_periods>`
for backwards compatibility, so the PV `SPEC:1:57:X` will return data that was streamed with `period=0` in Kafka.
Asking for period zero, as in a PV like `SPEC:0:57:X`, will return data for the *current* period.
::::
:::

## Meta-diagnostics

These readbacks provide feedback on the performance of the streaming system itself.

:::{dropdown} Streaming data rate statistics
### `EVENTMESSAGES`

The number of event messages (e.g. `ev44` schema) processed during the current run. This includes vetoed messages.

### `EVENTMODEFILEMB`

Mebibytes of event messages (e.g. `ev44` schema) processed during the current run. This includes vetoed messages.

### `EVENTMODEDATARATE`

The average data rate of event messages in the current run in `MiB/s`. This includes vetoed messages.
:::

:::{dropdown} Processing latencies
### `PROCESSINGLAG`

Estimated time difference, in seconds, between an event being emitted by the streaming hardware and being processed by the diagnostics IOC. This assumes that the clock in the streaming PC is reasonably well synchronised with the GPS clock used by the streaming hardware.

### `DIAGNOSTICSLAG`

The amount of time taken, in seconds, between diagnostic PV updates. After an event message is processed by the diagnostics IOC, the corresponding update will not be published to PVs until this interval next expires.
:::
