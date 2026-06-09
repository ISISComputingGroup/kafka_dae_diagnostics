use std::cmp::max;
use numpy::ndarray::{Array1, Array3};
use pyo3::{pyclass, pymethods, Py, PyAny};
use pyo3::types::{PyDict, PyTuple, PySuper};
use ahash::AHashMap;


struct FrameMetadata {

}

#[pyclass]
#[derive(Default)]
pub struct Data {
    spectra: Array3<f64>,
    callbacks: Vec<Py<PyAny>>,
    bin_boundaries: Array1<i32>,
    #[pyo3(get, set)]
    total_events: u64,
    #[pyo3(get, set)]
    total_event_messages: u64,
    #[pyo3(get, set)]
    total_event_megabytes: f64,
    #[pyo3(get, set)]
    largest_kafka_timestamp: f64,
    #[pyo3(get, set)]
    most_recent_kafka_timestamp: f64,
    #[pyo3(get, set)]
    start_time: f64,
    #[pyo3(get, set)]
    stop_time: f64,
    #[pyo3(get, set)]
    event_processing_lag: f64,
    frame_metadata: AHashMap<i64, FrameMetadata>,
    raw_frames_pd: Array1<u64>,
    good_frames_pd: Array1<u64>,
    #[pyo3(get, set)]
    raw_frames: u64,
    #[pyo3(get, set)]
    good_frames: u64,
    raw_uah_pd: Array1<f64>,
    good_uah_pd: Array1<f64>,
    #[pyo3(get, set)]
    raw_uah: f64,
    #[pyo3(get, set)]
    good_uah: f64,
    #[pyo3(get, set)]
    veto_mask: u32,
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
        ((self.largest_kafka_timestamp - self.start_time)).max(0.)
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
