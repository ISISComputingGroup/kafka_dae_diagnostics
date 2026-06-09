use crate::data::{Data, FrameMetadata};
use pyo3::PyResult;
use isis_streaming_data_types::{deserialize_message, DeserializedMessage};
use isis_streaming_data_types::flatbuffers_generated::events_ev44::Event44Message;
use isis_streaming_data_types::flatbuffers_generated::pulse_metadata_pu00::Pu00Message;

fn handle_event_topic_message(
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
        _ => {}
    }

    Ok(())
}

fn handle_ev44(data: &mut Data, partition: i64, msg: Event44Message) -> PyResult<()> {
    if let Some(metadata) = data.frame_metadata.get(&partition) {
        data.total_event_messages += 1;
        data.total_event_megabytes += 1.0;  // TODO

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