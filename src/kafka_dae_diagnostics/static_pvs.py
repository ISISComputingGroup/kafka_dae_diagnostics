import time

from p4p.server.thread import SharedPV

from p4p.nt import NTScalar

from kafka_dae_diagnostics.data import Data


class StaticPVs:
    def __init__(self, data: "Data"):
        self.total_events = SharedPV(nt=NTScalar(display=True, form=True), initial={
            "value": data.total_events,
            "display.precision": 0,
        })
        self.total_mevents = SharedPV(nt=NTScalar(display=True, form=True), initial={
            "value": data.mev,
            "display.precision": 6,
        })

        self.total_event_messages = SharedPV(nt=NTScalar(display=True, form=True), initial={
            "value": data.total_event_messages,
            "display.precision": 0
        })

        self.total_event_megabytes = SharedPV(nt=NTScalar(display=True, form=True), initial={
            "value": data.total_event_megabytes,
            "display.units": "MB",
            "display.precision": 3,
        })

        self.histogram_memory = SharedPV(nt=NTScalar(display=True, form=True), initial={
            "value": data.histogram_megabytes,
            "display.units": "MB",
            "display.precision": 3,
        })

        self.num_periods = SharedPV(nt=NTScalar(display=True, form=True), initial={
            "value": data.num_periods,
            "display.precision": 0,
        })

        self.num_spectra = SharedPV(nt=NTScalar(display=True, form=True), initial={
            "value": data.num_detectors,
            "display.precision": 0,
        })

        self.num_time_channels = SharedPV(nt=NTScalar(display=True, form=True), initial={
            "value": data.num_time_channels,
            "display.precision": 0,
        })

        self.count_rate = SharedPV(nt=NTScalar(display=True, form=True), initial={
            "value": data.num_time_channels,
            "display.units": "Mev/h",
            "display.precision": 3,
        })

        self.start_time = SharedPV(nt=NTScalar(display=True, form=True), initial={
            "value": data.start_time,
            "display.precision": 0,
        })

        self.run_duration = SharedPV(nt=NTScalar(display=True, form=True), initial={
            "value": data.duration,
            "display.units": "s",
            "display.precision": 1,
        })

        self.processing_lag = SharedPV(nt=NTScalar(display=True, form=True), initial={
            "value": data.processing_lag,
            "display.units": "s",
            "display.precision": 3,
        })

    def update_all(self, data: Data):
        self.total_events.post(data.total_events, timestamp=time.time())
        self.total_mevents.post(data.mev, timestamp=time.time())
        self.total_event_messages.post(data.total_event_messages, timestamp=time.time())
        self.total_event_megabytes.post(data.total_event_megabytes, timestamp=time.time())
        self.histogram_memory.post(data.histogram_megabytes, timestamp=time.time())
        self.num_periods.post(data.num_periods, timestamp=time.time())
        self.num_spectra.post(data.num_detectors, timestamp=time.time())
        self.num_time_channels.post(data.num_time_channels, timestamp=time.time())
        self.count_rate.post(data.mev_per_hour, timestamp=time.time())
        self.start_time.post(data.start_time, timestamp=time.time())
        self.run_duration.post(data.duration, timestamp=time.time())
        self.processing_lag.post(data.processing_lag, timestamp=time.time())