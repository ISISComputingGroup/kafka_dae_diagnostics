"""Static PVs."""

import time

from p4p.nt import NTScalar
from p4p.server.thread import SharedPV

from kafka_dae_diagnostics.data import Data


class StaticPVs:
    """Hold static (scalar) PV definitions."""

    def __init__(self, data: "Data") -> None:
        """Hold static (scalar) PV definitions."""
        self._last_update = time.time()
        self._last_update_data_size = data.total_event_megabytes

        self.total_events = SharedPV(
            nt=NTScalar(display=True, form=True),
            initial={
                "value": data.total_events,
                "display.precision": 0,
            },
        )
        self.total_mevents = SharedPV(
            nt=NTScalar(display=True, form=True),
            initial={
                "value": data.mev,
                "display.precision": 6,
            },
        )

        self.total_event_messages = SharedPV(
            nt=NTScalar(display=True, form=True),
            initial={"value": data.total_event_messages, "display.precision": 0},
        )

        self.total_event_megabytes = SharedPV(
            nt=NTScalar(display=True, form=True),
            initial={
                "value": data.total_event_megabytes,
                "display.units": "MiB",
                "display.precision": 3,
            },
        )

        self.histogram_memory = SharedPV(
            nt=NTScalar(display=True, form=True),
            initial={
                "value": data.histogram_megabytes,
                "display.units": "MiB",
                "display.precision": 3,
            },
        )

        self.num_periods = SharedPV(
            nt=NTScalar(display=True, form=True),
            initial={
                "value": data.num_periods,
                "display.precision": 0,
            },
        )

        self.num_spectra = SharedPV(
            nt=NTScalar(display=True, form=True),
            initial={
                "value": data.num_spectra,
                "display.precision": 0,
            },
        )

        self.num_time_channels = SharedPV(
            nt=NTScalar(display=True, form=True),
            initial={
                "value": data.num_time_channels,
                "display.precision": 0,
            },
        )

        self.count_rate = SharedPV(
            nt=NTScalar(display=True, form=True),
            initial={
                "value": data.num_time_channels,
                "display.units": "Mev/h",
                "display.precision": 3,
            },
        )

        self.data_rate = SharedPV(
            nt=NTScalar(display=True, form=True),
            initial={
                "value": data.average_data_rate,
                "display.units": "MiB/s",
                "display.precision": 3,
            },
        )

        self.start_time = SharedPV(
            nt=NTScalar(display=True, form=True),
            initial={
                "value": data.start_time,
                "display.precision": 0,
            },
        )

        self.stop_time = SharedPV(
            nt=NTScalar(display=True, form=True),
            initial={
                "value": data.start_time,
                "display.precision": 0,
            },
        )

        self.run_duration = SharedPV(
            nt=NTScalar(display=True, form=True),
            initial={
                "value": data.duration,
                "display.units": "s",
                "display.precision": 1,
            },
        )

        self.event_processing_lag = SharedPV(
            nt=NTScalar(display=True, form=True),
            initial={
                "value": data.event_processing_lag,
                "display.units": "s",
                "display.precision": 3,
            },
        )

        self.diagnostics_update_lag = SharedPV(
            nt=NTScalar(display=True, form=True),
            initial={
                "value": 0,
                "display.units": "s",
                "display.precision": 3,
            },
        )

    def update_all(self, data: Data) -> None:
        """Update all PVs with new data.

        Args:
            data (Data): New data.

        """
        now = time.time()
        self.total_events.post(data.total_events, timestamp=now)
        self.total_mevents.post(data.mev, timestamp=now)
        self.total_event_messages.post(data.total_event_messages, timestamp=now)
        self.total_event_megabytes.post(data.total_event_megabytes, timestamp=now)
        self.histogram_memory.post(data.histogram_megabytes, timestamp=now)
        self.num_periods.post(data.num_periods, timestamp=now)
        self.num_spectra.post(data.num_spectra, timestamp=now)
        self.num_time_channels.post(data.num_time_channels, timestamp=now)
        self.count_rate.post(data.mev_per_hour, timestamp=now)
        self.start_time.post(data.start_time, timestamp=now)
        self.stop_time.post(data.stop_time, timestamp=now)
        self.run_duration.post(data.duration, timestamp=now)
        self.event_processing_lag.post(data.event_processing_lag, timestamp=now)
        self.data_rate.post(data.average_data_rate, timestamp=now)

        diagnostics_update_lag = now - self._last_update
        self._last_update = now
        self.diagnostics_update_lag.post(diagnostics_update_lag, timestamp=now)
