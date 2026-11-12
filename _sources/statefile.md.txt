# Autosaved parameters

`kafka_dae_diagnostics` generally has very few user-settable parameters; most configuration is acquired dynamically
from Kafka.

For parameters that are user-settable via PVs, such as `TCB:LINEAR:*`, these are autosaved in a 'state file'. This
is conceptually similar to autosave in EPICS; parameters are saved at a regular interval (30s by default) and reloaded
when `kafka_dae_diagnostics` restarts.

If the state file is corrupt, it can safely be deleted - the side effect will be that parameters that *would* have
been autosaved will be lost and will need to be re-entered by setting the relevant PVs.
