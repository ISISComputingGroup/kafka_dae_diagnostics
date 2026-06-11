use ahash::AHashMap;
use isis_streaming_data_types::{deserialize_message, DeserializedMessage};
use isis_streaming_data_types::flatbuffers_generated::events_ev44::Event44Message;
use isis_streaming_data_types::flatbuffers_generated::pulse_metadata_pu00::Pu00Message;
use isis_streaming_data_types::flatbuffers_generated::run_start_pl72::RunStart;
use isis_streaming_data_types::flatbuffers_generated::run_stop_6s4t::RunStop;
use log::{info, warn};
use numpy::ndarray::{Array1, Array3};
use numpy::{PyArray, PyArray1, PyArray2};
use pyo3::prelude::*;
use crate::frame_metadata::FrameMetadata;
use numpy::{IntoPyArray};
use pyo3::impl_::callback::IntoPyCallbackOutput;
use pyo3::IntoPyObjectExt;
use crate::histogram::Histogram;

#[pyclass]
#[derive(Default)]
pub struct Data {
    histogram: Histogram,
    total_events: u64,
    total_event_messages: u64,
    total_event_bytes: u64,
    largest_kafka_timestamp: f64,
    most_recent_kafka_timestamp: f64,
    start_time: f64,
    stop_time: f64,
    event_processing_lag: f64,
    frame_metadata: AHashMap<i64, FrameMetadata>,
    raw_frames_pd: Array1<u64>,
    good_frames_pd: Array1<u64>,
    raw_frames: u64,
    good_frames: u64,
    raw_uah_pd: Array1<f64>,
    good_uah_pd: Array1<f64>,
    raw_uah: f64,
    good_uah: f64,
    veto_mask: u32,
}

impl Data {

    fn handle_ev44(&mut self, partition: i64, msg: Event44Message) -> PyResult<()> {
        if let Some(metadata) = self.frame_metadata.get(&partition) {
            if msg.reference_time().len() < 1 {
                warn!("ev44 message with no reference times; ignoring");
                return Ok(())
            }
            let ev44_timestamp_s = msg.reference_time().get(0) as f64 / 1_000_000_000.0;

            self.total_event_messages += 1;
            self.largest_kafka_timestamp = self.largest_kafka_timestamp.max(ev44_timestamp_s);
            self.most_recent_kafka_timestamp = ev44_timestamp_s;
            self.event_processing_lag = 0.;  // TODO

            let is_vetoed = (metadata.vetos & self.veto_mask) != 0;

            if is_vetoed {
                return Ok(())
            }

            if let Some(pixels) = msg.pixel_id() && let Some(tofs) = msg.time_of_flight() {
                let period = self.frame_metadata
                    .get(&partition)
                    .map(|meta| meta.period);

                if let Some(period) = period {
                    self.histogram.add_events(
                        period as usize,
                        &tofs.iter().collect::<Vec<_>>(),
                        &pixels.iter().collect::<Vec<_>>(),
                    );
                    self.total_events += pixels.len() as u64;
                } else {
                    warn!("ev44 message with no frame metadata; not histogramming events");
                }
            }
        }

        Ok(())
    }

    fn handle_pu00(&mut self, partition: i64, msg: Pu00Message) -> PyResult<()> {
        let period = msg.period_number().unwrap_or(0);
        let proton_charge = msg.proton_charge().unwrap_or(0.0);
        let vetos = msg.vetos().unwrap_or(0);

        self.raw_frames += 1;
        self.raw_uah += proton_charge as f64;

        self.frame_metadata.insert(partition, FrameMetadata {
            vetos, proton_charge, period
        });

        if let Some(raw_frames_pd) = self.raw_frames_pd.get_mut(period as usize) {
            *raw_frames_pd += 1;
        }
        if let Some(raw_uah_pd) = self.raw_uah_pd.get_mut(period as usize) {
            *raw_uah_pd += proton_charge as f64;
        }

        let is_vetoed = (vetos & self.veto_mask) != 0;

        if !is_vetoed {
            self.good_frames += 1;
            self.good_uah += proton_charge as f64;
            if let Some(good_frames_pd) = self.good_frames_pd.get_mut(period as usize) {
                *good_frames_pd += 1;
            }
            if let Some(good_uah_pd) = self.good_uah_pd.get_mut(period as usize) {
                *good_uah_pd += proton_charge as f64;
            }
        }

        Ok(())
    }

    fn handle_pl72(&mut self, msg: RunStart) -> PyResult<()> {
        let n_spectra = if let Some(det_spec_map) = msg.detector_spectrum_map() {
            det_spec_map.n_spectra()
        } else {
            1
        };

        info!("Run start (filename='{:?}', start_time={:?}, run_name='{:?}', instrument_name='{:?}', n_spectra={}",
        msg.filename(), msg.start_time(), msg.run_name(), msg.instrument_name(), n_spectra);

        // TODO: periods, detectors, time channels
        let periods = 1;
        let spectra = n_spectra as usize;
        let time_channels = 1000;

        self.histogram.reset(periods, spectra);
        self.histogram.change_bin_boundaries(Array1::linspace(0., 100_000_000., time_channels + 1).mapv(|f| f as i32).to_vec())?;

        self.raw_frames_pd = Array1::zeros(periods);
        self.good_frames_pd = Array1::zeros(periods);
        self.raw_uah_pd = Array1::zeros(periods);
        self.good_uah_pd = Array1::zeros(periods);

        Ok(())
    }

    fn handle_6s4t(&mut self,msg: RunStop) -> PyResult<()> {
        self.stop_time = msg.stop_time() as f64 / 1000.;
        Ok(())
    }
}

#[pymethods]
impl Data {
    #[new]
    fn new() -> Self {
        Self {
            ..Default::default()
        }
    }

    /// Process a message from Kafka, mutating the state of ``Self``
    /// to reflect the newly-processed messages.
    fn handle_msg(&mut self, bytes: &[u8], partition: i64) -> PyResult<()> {
        self.total_event_bytes += bytes.len() as u64;

        match deserialize_message(bytes) {
            Ok(DeserializedMessage::EventDataEv44(msg)) => {
                self.handle_ev44(partition, msg)?;
            }
            Ok(DeserializedMessage::PulseMetadataPu00(msg)) => {
                self.handle_pu00(partition, msg)?;
            }
            Ok(DeserializedMessage::RunStartPl72(msg)) => {
                self.handle_pl72(msg)?;
            }
            Ok(DeserializedMessage::RunStop6s4t(msg)) => {
                self.handle_6s4t(msg)?;
            }
            _ => {}
        }

        Ok(())
    }

    fn total_events(&self) -> u64 {
        self.total_events
    }

    fn mev(&self) -> f64 {
        self.total_events as f64 / 1_000_000.0
    }

    fn total_event_messages(&self) -> u64 {
        self.total_event_messages
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
        self.histogram.periods()
    }

    fn num_spectra(&self) -> usize {
        self.histogram.spectra()
    }

    fn num_time_channels(&self) -> usize {
        self.histogram.time_channels()
    }

    fn histogram_megabytes(&self) -> f64 {
        self.histogram.megabytes()
    }

    fn total_event_megabytes(&self) -> f64 {
        self.total_event_bytes as f64 / (1024.0 * 1024.0)
    }

    fn good_frames_pd<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyArray1<u64>>> {
        Ok(self.good_frames_pd.clone().into_pyarray(py))
    }

    fn raw_frames_pd<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyArray1<u64>>> {
        Ok(self.raw_frames_pd.clone().into_pyarray(py))
    }

    fn good_uah_pd<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyArray1<f64>>> {
        Ok(self.good_uah_pd.clone().into_pyarray(py))
    }

    fn raw_uah_pd<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyArray1<f64>>> {
        Ok(self.raw_uah_pd.clone().into_pyarray(py))
    }

    fn good_frames(&self) -> u64 {
        self.good_frames
    }

    fn raw_frames(&self) -> u64 {
        self.raw_frames
    }

    fn good_uah(&self) -> f64 {
        self.good_uah
    }

    fn raw_uah(&self) -> f64 {
        self.raw_uah
    }

    fn start_time(&self) -> f64 {
        self.start_time
    }

    fn stop_time(&self) -> f64 {
        self.stop_time
    }

    fn event_processing_lag(&self) -> f64 {
        0.0  // TODO
    }

    fn average_data_rate(&self) -> f64 {
        let duration = self.duration();
        if duration == 0.0 {
            0.0
        } else {
            self.total_event_megabytes() / duration
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

    fn bin_boundaries<'py>(&self, py: Python<'py>) -> Bound<'py, PyArray1<i32>> {
        PyArray1::from_slice(py, self.histogram.bin_boundaries())
    }

    fn bin_centres<'py>(&self, py: Python<'py>) -> Bound<'py, PyArray1<f64>> {
        PyArray1::from_slice(py, self.histogram.bin_centres())
    }

    fn histogram_data<'py>(&self, py: Python<'py>, period: usize, spectrum: usize) -> Bound<'py, PyArray1<f64>> {
        PyArray1::from_array(py, &self.histogram.data(period, spectrum))
    }
}
