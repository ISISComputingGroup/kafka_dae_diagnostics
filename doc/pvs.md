# PVs

This page describes the data held by each PV served by this IOC.

:::{seealso}
{py:class}`kafka_dae_diagnostics.data.Data` for the data-model used by this IOC.
:::

## Static PVs

### `EVENTS` / `TOTALEVENTS`

The total number of non-vetoed neutron events seen during the current run.

### `MEVENTS`

Millions of events seen during the current run. Equivalent to `EVENTS` divided by {math}`10^6`.

### `EVENTMESSAGES`

The number of event messages (e.g. `ev44` schema) processed during the current run. This includes vetoed messages.

### `EVENTMODEFILEMB`

Mebibytes of event messages (e.g. `ev44` schema) processed during the current run. This includes vetoed messages.

### `COUNTRATE`

The average count rate of the current run, in millions of events per hour.

This only includes *good* events.

### `EVENTMODEDATARATE`

The average data rate of event messages in the current run in `MiB/s`. This includes vetoed messages.

### `HISTMEMORY`

Memory used by diagnostic histograms, in `MiB`.

This is `num_periods * num_spectra * num_time_channels * 8 bytes`.

### `GOODFRAMES` / `RAWFRAMES`

Number of ISIS frames in the current run.

`GOODFRAMES` counts only non-vetoed frames, `RAWFRAMES` counts both vetoed and non-vetoed frames.

### `GOODFRAMES_PD` / `RAWFRAMES_PD`

Array of good/raw ISIS frames in the current run, keyed by period number. The array has length `NUMPERIODS`.

:::{note}
The sum of this array may differ from `GOODFRAMES` if frames with an invalid period number are received. Those frames will be included in `GOODFRAMES`, but will not appear in `GOODFRAMES_PD`.
:::

### `GOODUAH` / `RAWUAH`

Accumulated microAmp-hours in the current run.

`GOODUAH` counts only microAmp-hours for the non-vetoed frames, `RAWUAH` counts both vetoed and non-vetoed frames.

### `GOODUAH_PD` / `RAWUAH_PD`

Array of good/raw microAmp-hours in the current run, keyed by period number. The array has length `NUMPERIODS`.

:::{note}
The sum of `GOODUAH_PD` may differ from `GOODUAH` if frames with an invalid period number are received. Those frames will be included in `GOODUAH`, but will not appear in `GOODUAH_PD`.
:::

### `NUMPERIODS`

Number of periods configured in the current run. Valid period numbers are strictly less than this number.

### `NUMSPECTRA`

Number of spectra configured for the diagnostic histogram of the current run.

### `NUMTIMECHANNELS`

Number of time channels configured for the diagnostic histogram of the current run.

### `START_TIME` / `STOP_TIME`

The most recent run-start and run-stop timestamps.

### `RUNDURATION`

Duration, in seconds, of the current or most recent run.

### `PROCESSINGLAG`

Estimated time difference, in seconds, between an event being emitted by the streaming hardware and being processed by the diagnostics IOC. This assumes that the clock in the streaming PC is reasonably well synchronised with the GPS clock used by the streaming hardware.

### `DIAGNOSTICSLAG`

The amount of time taken, in seconds, between diagnostic PV updates. After an event message is processed by the diagnostics IOC, the corresponding update will not be published to PVs until this interval next expires.

## Dynamic PVs

These PVs are created on-the-fly by `kafka_dae_diagnostics`. The PV will not exist if
the requested period or spectrum is outside the range currently configured.

### `SPEC:<period>:<spectrum>:X`

These PVs provide the X points (the time-of-flight bin centres) for a time of flight spectrum. This array has length `num_time_channels`.

### `SPEC:<period>:<spectrum>:Y`

These PVs provide the Y points (the number of counts in the relevant bin) for a time of flight spectrum. This array has length `num_time_channels`.
