# Kafka DAE Diagnostics

This module provides a `KDAEDIAG` IOC, which acts as a Kafka event-stream consumer and publishes
a number of diagnostic PVs (for example spectra plots) over EPICS PV Access.

The diagnostics IOC can be launched with:

```bash
set EPICS_PVAS_INTF_ADDR_LIST=127.0.0.1
set EPICS_PVA_BEACON_ADDR_LIST=127.255.255.255
kdaediag --config config.toml
```

```{toctree}
:titlesonly:
:caption: Developer Information
:glob:

local_development
architectural_decision_records
```


```{toctree}
:titlesonly:
:caption: Reference
:glob:

_api
```
