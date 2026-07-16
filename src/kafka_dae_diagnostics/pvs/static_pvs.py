"""Static PVs."""

import time

from p4p.nt import NTEnum, NTScalar
from p4p.server import StaticProvider
from p4p.server.thread import SharedPV

from kafka_dae_diagnostics.data import Data, RunState


def static_pv_provider(prefix: str, data: Data) -> StaticProvider:
    """:py:obj:`p4p` static PV provider.

    Args:
        prefix: PV prefix
        data: The data to serve

    """
    static_pvs = StaticPVs(data)
    static_provider = StaticProvider()
    static_provider.add(f"{prefix}RUNSTATE", static_pvs.run_state)
    static_provider.add(f"{prefix}RUNSTATE_STR", static_pvs.run_state_str)

    static_provider.add(f"{prefix}EVENTS", static_pvs.total_events)
    static_provider.add(f"{prefix}MEVENTS", static_pvs.total_mevents)
    static_provider.add(f"{prefix}TOTALCOUNTS", static_pvs.total_events)
    static_provider.add(f"{prefix}EVENTMESSAGES", static_pvs.total_event_messages)
    static_provider.add(f"{prefix}EVENTMODEFILEMB", static_pvs.total_event_megabytes)
    static_provider.add(f"{prefix}COUNTRATE", static_pvs.count_rate)
    static_provider.add(f"{prefix}EVENTMODEDATARATE", static_pvs.data_rate)
    static_provider.add(f"{prefix}HISTMEMORY", static_pvs.histogram_memory)

    static_provider.add(f"{prefix}GOODFRAMES", static_pvs.good_frames)
    static_provider.add(f"{prefix}RAWFRAMES", static_pvs.raw_frames)
    static_provider.add(f"{prefix}GOODFRAMES_PD", static_pvs.good_frames_pd)
    static_provider.add(f"{prefix}RAWFRAMES_PD", static_pvs.raw_frames_pd)
    static_provider.add(f"{prefix}GOODUAH", static_pvs.good_uah)
    static_provider.add(f"{prefix}RAWUAH", static_pvs.raw_uah)
    static_provider.add(f"{prefix}GOODUAH_PD", static_pvs.good_uah_pd)
    static_provider.add(f"{prefix}RAWUAH_PD", static_pvs.raw_uah_pd)

    static_provider.add(f"{prefix}NUMPERIODS", static_pvs.num_periods)
    static_provider.add(f"{prefix}NUMSPECTRA", static_pvs.num_spectra)
    static_provider.add(f"{prefix}NUMTIMECHANNELS", static_pvs.num_time_channels)

    static_provider.add(f"{prefix}START_TIME", static_pvs.start_time)
    static_provider.add(f"{prefix}STARTTIME", static_pvs.start_time_str)

    static_provider.add(f"{prefix}STOP_TIME", static_pvs.stop_time)
    static_provider.add(f"{prefix}STOPTIME", static_pvs.stop_time_str)

    static_provider.add(f"{prefix}RUNDURATION", static_pvs.run_duration)

    static_provider.add(f"{prefix}PROCESSINGLAG", static_pvs.event_processing_lag)
    static_provider.add(f"{prefix}DIAGNOSTICSLAG", static_pvs.diagnostics_update_lag)
    static_provider.add(
        f"{prefix}SECSSINCELASTEVENTMESSAGE", static_pvs.seconds_since_last_event_message
    )

    static_provider.add(f"{prefix}VETO:RECENT:PERCENT", static_pvs.recent_veto_percentages)
    static_provider.add(f"{prefix}VETO:RECENT:COUNT", static_pvs.recent_veto_count)
    static_provider.add(f"{prefix}VETO:RUN:PERCENT", static_pvs.run_veto_percentages)
    static_provider.add(f"{prefix}VETO:RUN:COUNT", static_pvs.run_veto_count)
    static_provider.add(f"{prefix}VETO:ENABLED", static_pvs.enabled_vetos_array)
    static_provider.add(f"{prefix}VETO:NAMES", static_pvs.veto_names_array)

    data.callbacks["static-callbacks"] = lambda: static_pvs.update_all(data)

    return static_provider


class StaticPVs:
    """Hold static PV definitions."""

    def __init__(self, data: "Data") -> None:
        """Hold static PV definitions."""
        self._last_update = time.time()

        self.run_state = SharedPV(
            nt=NTEnum(display=True),
            initial={
                "choices": [x.name for x in RunState],
                "index": data.run_state.value,
            },
        )
        self.run_state_str = SharedPV(
            nt=NTScalar("s", display=True, form=True),
            initial={
                "value": data.run_state.name,
            },
        )

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
                "display.units": "MEv",
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

        self.good_frames_pd = SharedPV(
            nt=NTScalar("al", display=True, form=True),
            initial={
                "value": data.good_frames_pd,
                "display.units": "frames",
                "display.precision": 0,
            },
        )

        self.raw_frames_pd = SharedPV(
            nt=NTScalar("al", display=True, form=True),
            initial={
                "value": data.raw_frames_pd,
                "display.units": "frames",
                "display.precision": 0,
            },
        )

        self.good_frames = SharedPV(
            nt=NTScalar(display=True, form=True),
            initial={
                "value": data.good_frames,
                "display.units": "frames",
                "display.precision": 0,
            },
        )

        self.raw_frames = SharedPV(
            nt=NTScalar(display=True, form=True),
            initial={
                "value": data.raw_frames,
                "display.units": "frames",
                "display.precision": 0,
            },
        )

        self.good_uah_pd = SharedPV(
            nt=NTScalar("ad", display=True, form=True),
            initial={
                "value": data.good_uah_pd,
                "display.units": "uAh",
                "display.precision": 0,
            },
        )

        self.raw_uah_pd = SharedPV(
            nt=NTScalar("ad", display=True, form=True),
            initial={
                "value": data.raw_uah_pd,
                "display.units": "uAh",
                "display.precision": 0,
            },
        )

        self.good_uah = SharedPV(
            nt=NTScalar(display=True, form=True),
            initial={
                "value": data.good_uah,
                "display.units": "uAh",
                "display.precision": 3,
            },
        )

        self.raw_uah = SharedPV(
            nt=NTScalar(display=True, form=True),
            initial={
                "value": data.raw_uah,
                "display.units": "uAh",
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
                "value": data.count_rate,
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

        self.start_time_str = SharedPV(
            nt=NTScalar("s", display=True, form=True),
            initial={
                "value": data.start_time_str,
            },
        )

        self.stop_time = SharedPV(
            nt=NTScalar(display=True, form=True),
            initial={
                "value": data.stop_time,
                "display.precision": 0,
            },
        )

        self.stop_time_str = SharedPV(
            nt=NTScalar("s", display=True, form=True),
            initial={
                "value": data.stop_time_str,
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

        self.seconds_since_last_event_message = SharedPV(
            nt=NTScalar(display=True, form=True),
            initial={
                "value": data.seconds_since_last_event_message,
                "display.units": "s",
                "display.precision": 3,
            },
        )

        self.recent_veto_percentages = SharedPV(
            nt=NTScalar("ad", display=True, form=True),
            initial={
                "value": data.veto_diagnostics.get_recent_veto_percentages(),
                "display.units": "%",
                "display.precision": 2,
            },
        )
        self.run_veto_percentages = SharedPV(
            nt=NTScalar("ad", display=True, form=True),
            initial={
                "value": data.veto_diagnostics.get_run_veto_percentages(),
                "display.units": "%",
                "display.precision": 2,
            },
        )
        self.recent_veto_count = SharedPV(
            nt=NTScalar("ad", display=True, form=True),
            initial={
                "value": data.veto_diagnostics.get_recent_veto_count(),
                "display.units": "frames",
                "display.precision": 0,
            },
        )
        self.run_veto_count = SharedPV(
            nt=NTScalar("ad", display=True, form=True),
            initial={
                "value": data.veto_diagnostics.get_run_veto_count(),
                "display.units": "frames",
                "display.precision": 0,
            },
        )
        self.enabled_vetos_array = SharedPV(
            nt=NTScalar("al", display=True, form=True),
            initial={
                "value": data.enabled_vetos_array,
                "display.units": "",
                "display.precision": 0,
            },
        )
        self.veto_names_array = SharedPV(
            nt=NTScalar("as", display=True, form=True),
            initial={
                "value": data.veto_names_array,
            },
        )

    def update_all(self, data: Data) -> None:
        """Update all PVs with new data.

        Args:
            data (Data): New data.

        """
        now = time.time()

        run_state = data.run_state
        self.run_state.post(run_state.value, timestamp=now)
        self.run_state_str.post(run_state.name, timestamp=now)

        self.total_events.post(data.total_events, timestamp=now)
        self.total_mevents.post(data.mev, timestamp=now)
        self.total_event_messages.post(data.total_event_messages, timestamp=now)
        self.total_event_megabytes.post(data.total_event_megabytes, timestamp=now)
        self.histogram_memory.post(data.histogram_megabytes, timestamp=now)

        self.good_frames.post(data.good_frames, timestamp=now)
        self.raw_frames.post(data.raw_frames, timestamp=now)
        self.good_frames_pd.post(data.good_frames_pd, timestamp=now)
        self.raw_frames_pd.post(data.raw_frames_pd, timestamp=now)
        self.good_uah.post(data.good_uah, timestamp=now)
        self.raw_uah.post(data.raw_uah, timestamp=now)
        self.good_uah_pd.post(data.good_uah_pd, timestamp=now)
        self.raw_uah_pd.post(data.raw_uah_pd, timestamp=now)

        self.num_periods.post(data.num_periods, timestamp=now)
        self.num_spectra.post(data.num_spectra, timestamp=now)
        self.num_time_channels.post(data.num_time_channels, timestamp=now)
        self.count_rate.post(data.mev_per_hour, timestamp=now)
        self.start_time.post(data.start_time, timestamp=now)
        self.start_time_str.post(data.start_time_str, timestamp=now)
        self.stop_time.post(data.stop_time, timestamp=now)
        self.stop_time_str.post(data.stop_time_str, timestamp=now)
        self.run_duration.post(data.duration, timestamp=now)
        self.event_processing_lag.post(data.event_processing_lag, timestamp=now)
        self.seconds_since_last_event_message.post(
            data.seconds_since_last_event_message, timestamp=now
        )
        self.data_rate.post(data.average_data_rate, timestamp=now)

        self.recent_veto_percentages.post(
            data.veto_diagnostics.get_recent_veto_percentages(), timestamp=now
        )
        self.run_veto_percentages.post(
            data.veto_diagnostics.get_run_veto_percentages(), timestamp=now
        )
        self.recent_veto_count.post(data.veto_diagnostics.get_recent_veto_count(), timestamp=now)
        self.run_veto_count.post(data.veto_diagnostics.get_run_veto_count(), timestamp=now)
        self.enabled_vetos_array.post(data.enabled_vetos_array, timestamp=now)
        self.veto_names_array.post(data.veto_names_array, timestamp=now)

        diagnostics_update_lag = now - self._last_update
        self._last_update = now
        self.diagnostics_update_lag.post(diagnostics_update_lag, timestamp=now)
