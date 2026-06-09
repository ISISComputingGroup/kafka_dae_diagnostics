use ahash::AHashMap;
use numpy::ndarray::{Array1, Array3};
use pyo3::{Py, PyAny, pyclass, pymethods};

pub(crate) struct FrameMetadata {
    pub(crate) vetos: u32,
    pub(crate) proton_charge: f32,
    pub(crate) period: u32,
}

#[pyclass]
#[derive(Default)]
pub struct Data {
    pub(crate) spectra: Array3<f64>,
    pub(crate) callbacks: Vec<Py<PyAny>>,
    pub(crate) bin_boundaries: Array1<i32>,
    pub(crate) total_events: u64,
    pub(crate) total_event_messages: u64,
    pub(crate) total_event_megabytes: f64,
    pub(crate) largest_kafka_timestamp: f64,
    pub(crate) most_recent_kafka_timestamp: f64,
    pub(crate) start_time: f64,
    pub(crate) stop_time: f64,
    pub(crate) event_processing_lag: f64,
    pub(crate) frame_metadata: AHashMap<i64, FrameMetadata>,
    pub(crate) raw_frames_pd: Array1<u64>,
    pub(crate) good_frames_pd: Array1<u64>,
    pub(crate) raw_frames: u64,
    pub(crate) good_frames: u64,
    pub(crate) raw_uah_pd: Array1<f64>,
    pub(crate) good_uah_pd: Array1<f64>,
    pub(crate) raw_uah: f64,
    pub(crate) good_uah: f64,
    pub(crate) veto_mask: u32,
}

#[pymethods]
impl Data {
    #[new]
    fn new() -> Self {
        Data {
            ..Default::default()
        }
    }

    fn mev(&self) -> f64 {
        self.total_events as f64 / 1_000_000.0
    }

    fn duration(&self) -> f64 {
        (self.largest_kafka_timestamp - self.start_time).max(0.)
    }

    fn mev_per_hour(&self) -> f64 {
        let duration = self.duration();
        if duration == 0.0 {
            0.0
        } else {
            (self.mev() / duration) * 3600.0
        }
    }

    fn num_periods(&self) -> usize {
        self.spectra.shape()[0]
    }

    fn num_spectra(&self) -> usize {
        self.spectra.shape()[1]
    }

    fn num_time_channels(&self) -> usize {
        self.spectra.shape()[2]
    }

    fn histogram_megabytes(&self) -> f64 {
        self.spectra.len() as f64 * 8.0 / (1024.0 * 1024.0)
    }

    fn average_data_rate(&self) -> f64 {
        let duration = self.duration();
        if duration == 0.0 {
            0.0
        } else {
            self.total_event_megabytes / duration
        }
    }

    fn count_rate(&self) -> f64 {
        let duration = self.duration();
        if duration == 0.0 {
            0.0
        } else {
            (self.total_events as f64 * 3600.0) / (duration * 1_000_000.0)
        }
    }
}
