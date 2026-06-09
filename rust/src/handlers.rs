use crate::data::{Data, FrameMetadata};
use pyo3::PyResult;
use isis_streaming_data_types::{deserialize_message, DeserializedMessage};
use isis_streaming_data_types::flatbuffers_generated::events_ev44::Event44Message;
use isis_streaming_data_types::flatbuffers_generated::pulse_metadata_pu00::Pu00Message;
use isis_streaming_data_types::flatbuffers_generated::run_start_pl72::RunStart;
use isis_streaming_data_types::flatbuffers_generated::run_stop_6s4t::RunStop;
use log::{info, warn};
use numpy::ndarray::{Array1, Array3};

fn handle_message(
    data: &mut Data,
    bytes: &[u8],
    partition: i64
) -> PyResult<()> {
    match deserialize_message(bytes) {
        Ok(DeserializedMessage::EventDataEv44(msg)) => {
            handle_ev44(data, partition, msg)?;
        }
        Ok(DeserializedMessage::PulseMetadataPu00(msg)) => {
            handle_pu00(data, partition, msg)?;
        }
        Ok(DeserializedMessage::RunStartPl72(msg)) => {
            handle_pl72(data, partition, msg)?;
        }
        Ok(DeserializedMessage::RunStop6s4t(msg)) => {
            handle_6s4t(data, partition, msg)?;
        }
        _ => {}
    }

    Ok(())
}

fn handle_ev44(data: &mut Data, partition: i64, msg: Event44Message) -> PyResult<()> {
    if let Some(metadata) = data.frame_metadata.get(&partition) {
        if msg.reference_time().len() < 1 {
            warn!("ev44 message with no reference times; ignoring");
            return Ok(())
        }
        let ev44_timestamp_s = msg.reference_time().get(0) as f64 / 1_000_000_000.0;

        data.total_event_messages += 1;
        data.total_event_megabytes += 1.0;  // TODO
        data.largest_kafka_timestamp = data.largest_kafka_timestamp.max(ev44_timestamp_s);
        data.most_recent_kafka_timestamp = ev44_timestamp_s;
        data.event_processing_lag = 0.;  // TODO

        let is_vetoed = (metadata.vetos & data.veto_mask) != 0;

        if is_vetoed {
            return Ok(())
        }

        if let Some(pixels) = msg.pixel_id() && let Some(tofs) = msg.time_of_flight() {
            // TODO: histogramming

            data.total_events += pixels.len() as u64;
        }
    }

    Ok(())
}

fn handle_pu00(data: &mut Data, partition: i64, msg: Pu00Message) -> PyResult<()> {
    let period = msg.period_number().unwrap_or(0);
    let proton_charge = msg.proton_charge().unwrap_or(0.0);
    let vetos = msg.vetos().unwrap_or(0);

    data.raw_frames += 1;
    data.raw_uah += proton_charge as f64;

    data.frame_metadata.insert(partition, FrameMetadata {
        vetos, proton_charge, period
    });

    if let Some(raw_frames_pd) = data.raw_frames_pd.get_mut(period as usize) {
        *raw_frames_pd += 1;
    }
    if let Some(raw_uah_pd) = data.raw_uah_pd.get_mut(period as usize) {
        *raw_uah_pd += proton_charge as f64;
    }

    let is_vetoed = (vetos & data.veto_mask) != 0;

    if !is_vetoed {
        data.good_frames += 1;
        data.good_uah += proton_charge as f64;
        if let Some(good_frames_pd) = data.good_frames_pd.get_mut(period as usize) {
            *good_frames_pd += 1;
        }
        if let Some(good_uah_pd) = data.good_uah_pd.get_mut(period as usize) {
            *good_uah_pd += proton_charge as f64;
        }
    }

    Ok(())
}

fn handle_pl72(data: &mut Data, partition: i64, msg: RunStart) -> PyResult<()> {
    let n_spectra = if let Some(det_spec_map) = msg.detector_spectrum_map() {
        det_spec_map.n_spectra()
    } else {
        1
    };

    info!("Run start (filename='{:?}', start_time={:?}, run_name='{:?}', instrument_name='{:?}', n_spectra={}",
        msg.filename(), msg.start_time(), msg.run_name(), msg.instrument_name(), n_spectra);

    // TODO: periods, detectors, time channels
    let periods = 1;
    let detectors = n_spectra as usize;
    let time_channels = 1000;

    data.spectra = Array3::zeros((periods, detectors, time_channels));

    data.raw_frames_pd = Array1::zeros(periods);
    data.good_frames_pd = Array1::zeros(periods);
    data.raw_uah_pd = Array1::zeros(periods);
    data.good_uah_pd = Array1::zeros(periods);

    // TODO: consumer assignment

    Ok(())
}

fn handle_6s4t(data: &mut Data, partition: i64, msg: RunStop) -> PyResult<()> {
    Ok(())
}
