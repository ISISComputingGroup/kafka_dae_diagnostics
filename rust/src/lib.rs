use numpy::{PyReadonlyArray1, PyReadwriteArray2};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

fn tof_bin_event(tof: i32, bin_boundaries: &[i32]) -> Option<usize> {
    let partition_point = bin_boundaries.partition_point(|&t| tof >= t);
    if partition_point == 0 || partition_point >= bin_boundaries.len() {
        None
    } else {
        Some(partition_point - 1)
    }
}

fn tof_bin_event_linear(tof: i32, start: i32, stop: i32, step: i32) -> Option<usize> {
    if tof < start || tof >= stop {
        return None;
    }
    Some(((tof - start) / step) as usize)
}

/// Bin events into a spectrum array, specified by
/// bin-edge boundaries. The bin edges must be sorted;
/// the result of this function is meaningless for
/// non-sorted bin-edges.
///
/// This function does not release the GIL as it mutates
/// a *view* onto a numpy-allocated array. We cannot allow
/// other python threads to concurrently modify that array.
#[pyfunction]
fn bin_events_into_spectrum(
    mut histogram: PyReadwriteArray2<f64>,
    event_tofs: PyReadonlyArray1<i32>,
    pixel_ids: PyReadonlyArray1<i32>,
    tof_bin_boundaries: PyReadonlyArray1<i32>,
) -> PyResult<()> {
    let mut histogram = histogram.as_array_mut();
    let boundaries = tof_bin_boundaries.as_slice()?;
    let event_tofs = event_tofs.as_slice()?;
    let pixel_ids = pixel_ids.as_slice()?;

    if boundaries.len() < 2 {
        return Err(PyValueError::new_err(
            "Bin boundaries must have length >= 2",
        ));
    }
    if event_tofs.len() != pixel_ids.len() {
        return Err(PyValueError::new_err(
            "Events TOFs and pixel_ids must have the same length",
        ));
    }

    event_tofs
        .iter()
        .zip(pixel_ids.iter())
        .for_each(|(&tof, &pixel)| {
            if let Some(tof_bin) = tof_bin_event(tof, boundaries)
                && let Some(e) = histogram.get_mut((pixel as usize, tof_bin))
            {
                *e += 1.0
            }
        });

    Ok(())
}

/// Bin events into a spectrum array, specified by
/// a (start, stop, step) set of parameters.
///
/// This function does not release the GIL as it mutates
/// a *view* onto a numpy-allocated array. We cannot allow
/// other python threads to concurrently modify that array.
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
    let event_tofs = event_tofs.as_slice()?;
    let pixel_ids = pixel_ids.as_slice()?;

    if tof_bin_step == 0 {
        return Err(PyValueError::new_err("Must have a positive TOF bin step"));
    }

    if tof_bin_stop <= tof_bin_start {
        return Err(PyValueError::new_err(
            "TOF binning stop must be larger than start",
        ));
    }

    if event_tofs.len() != pixel_ids.len() {
        return Err(PyValueError::new_err(
            "Events TOFs and pixel_ids must have the same length",
        ));
    }

    event_tofs
        .iter()
        .zip(pixel_ids.iter())
        .for_each(|(&tof, &pixel)| {
            if let Some(tof_bin) =
                tof_bin_event_linear(tof, tof_bin_start, tof_bin_stop, tof_bin_step)
                && let Some(e) = histogram.get_mut((pixel as usize, tof_bin))
            {
                *e += 1.0
            }
        });

    Ok(())
}

/// Overall rust module for all rust helpers.
#[pymodule]
fn _kdaediag_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(bin_events_into_spectrum, m)?)?;
    m.add_function(wrap_pyfunction!(bin_events_into_spectrum_linear, m)?)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tof_bin_event() {
        let bin_edges = [2, 4, 6];
        assert_eq!(tof_bin_event(0, &bin_edges), None);
        assert_eq!(tof_bin_event(1, &bin_edges), None);
        assert_eq!(tof_bin_event(2, &bin_edges), Some(0));
        assert_eq!(tof_bin_event(3, &bin_edges), Some(0));
        assert_eq!(tof_bin_event(4, &bin_edges), Some(1));
        assert_eq!(tof_bin_event(5, &bin_edges), Some(1));
        assert_eq!(tof_bin_event(6, &bin_edges), None);
        assert_eq!(tof_bin_event(7, &bin_edges), None);
    }

    #[test]
    fn test_tof_bin_event_linear() {
        let (start, stop, step) = (2, 6, 2);
        assert_eq!(tof_bin_event_linear(0, start, stop, step), None);
        assert_eq!(tof_bin_event_linear(1, start, stop, step), None);
        assert_eq!(tof_bin_event_linear(2, start, stop, step), Some(0));
        assert_eq!(tof_bin_event_linear(3, start, stop, step), Some(0));
        assert_eq!(tof_bin_event_linear(4, start, stop, step), Some(1));
        assert_eq!(tof_bin_event_linear(5, start, stop, step), Some(1));
        assert_eq!(tof_bin_event_linear(6, start, stop, step), None);
        assert_eq!(tof_bin_event_linear(7, start, stop, step), None);
    }
}
