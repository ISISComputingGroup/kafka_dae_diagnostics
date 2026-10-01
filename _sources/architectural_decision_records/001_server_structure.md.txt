# 1. EPICS Server structure

## Status

Current

## Context

This module aims to deliver "DAE Diagnostics", in the forms of:
- Spectra plots
- Information about vetoes
- Event-rate information
- "Detector diagnostics"

Of these, the most demanding case is serving spectra plots; there can be large numbers of spectra
corresponding to different detectors and periods, and each spectrum has the potential to
update at a high rate as new neutron events are histogrammed into the spectrum.

Use cases for DAE spectra include:
- A user visually monitoring a spectrum from a GUI. This means it must be possible for a client to
monitor a spectrum that it is interested in.
- Scripted clients, which will retrieve spectra programmatically, for example:
  * {py:obj}`ibex_bluesky_core.devices.dae.DaeSpectra`
  * {py:obj}`genie.get_spectrum`

Several approaches to serving spectra were considered:

### Static PVs ({py:obj}`fastcs`)

In this approach, this service would statically create PVs for each spectrum/period combination, and
update them as new neutron events arrive. {py:obj}`fastcs` can serve arrays over both CA and PVA
simultaneously.

This approach is simple, but very wasteful performance-wise: all spectra would be being updated
constantly, even if no client was interested. The performance of this approach is not viable, even
for modest numbers of spectra.

### CA Server ({py:obj}`pcaspy`)

PCAS & pcaspy are deprecated, and there is not presently a route to "bridge" a PCAS/pcaspy server to
PVA. Support for PVA is considered a requirement, so this option is discarded.

### PVA server ({py:obj}`p4p`)

This approach uses the {py:obj}`p4p.server.DynamicProvider` to dynamically serve PVA PVs to subscribed
clients. It is conceptually similar to the `pcaspy` approach above, but would **only** support PVA.

## Decision

Spectra will be served via {py:obj}`p4p` using {py:obj}`p4p.server.DynamicProvider`.

## Consequences

- Spectra will _only_ be available over PVA. No CA support will be available.
  - The existing {external+ibex_developers_manual:doc}`Eclipse-based IBEX client <Client>` can support
PVA in OPIs.
  - {py:obj}`ibex_bluesky_core` and {py:obj}`genie` clients already have support for PVA but will need updates
to use it to acquire spectra.
- Spectra will be served by a {py:obj}`p4p` server.
