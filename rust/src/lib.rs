use pyo3::prelude::*;
use numpy::{PyReadonlyArray1, PyReadwriteArray2};

fn tof_bin_event(tof: i32, bin_boundaries: &[i32]) -> Option<usize> {
    if tof < *bin_boundaries.first()? || tof >= *bin_boundaries.last()? {
        return None
    }

    let mut lower_bound = 0;
    let mut upper_bound = bin_boundaries.len() - 1;
    while upper_bound - lower_bound > 1 {
        let mid = (upper_bound + lower_bound) / 2;
        if tof >= bin_boundaries[mid] {
            lower_bound = mid;
        } else {
            upper_bound = mid;
        }
    }
    Some(lower_bound)
}

fn tof_bin_event_linear(tof: i32, start: i32, stop: i32, step: i32) -> Option<usize> {
    if tof < start || tof >= stop {
        return None
    }
    Some(((tof - start) / step) as usize)
}

#[pyfunction]
fn bin_events_into_spectrum(
    mut histogram: PyReadwriteArray2<f64>,
    event_tofs: PyReadonlyArray1<i32>,
    pixel_ids: PyReadonlyArray1<i32>,
    tof_bin_boundaries: PyReadonlyArray1<i32>,
) -> PyResult<()> {
    let mut histogram = histogram.as_array_mut();
    let boundaries = tof_bin_boundaries.as_slice()?;

    event_tofs.as_array()
        .iter()
        .zip(pixel_ids.as_array().iter())
        .for_each(|(&tof, &pixel)| {
            if let Some(tof_bin) = tof_bin_event(tof, boundaries)
                && let Some(e) = histogram.get_mut((pixel as usize, tof_bin)) {
                *e += 1.0
            }
        });

    Ok(())
}

#[pyfunction]
fn bin_events_into_spectrum_linear(
    mut histogram: PyReadwriteArray2<f64>,
    event_tofs: PyReadonlyArray1<i32>,
    pixel_ids: PyReadonlyArray1<i32>,
    tof_bin_start: i32,
    tof_bin_stop: i32,
    tof_bin_step: i32,
) -> PyResult<()> {
    let mut histogram = histogram.as_array_mut();

    event_tofs.as_array()
        .iter()
        .zip(pixel_ids.as_array().iter())
        .for_each(|(&tof, &pixel)| {
            if let Some(tof_bin) = tof_bin_event_linear(tof, tof_bin_start, tof_bin_stop, tof_bin_step)
                && let Some(e) = histogram.get_mut((pixel as usize, tof_bin)) {
                *e += 1.0
            }
        });

    Ok(())
}

#[pymodule]
fn _kdaediag_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(bin_events_into_spectrum, m)?)?;
    m.add_function(wrap_pyfunction!(bin_events_into_spectrum_linear, m)?)?;

    Ok(())
}
