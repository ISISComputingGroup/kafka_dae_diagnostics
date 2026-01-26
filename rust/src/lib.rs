use numpy::{PyReadonlyArray1, PyReadwriteArray2, ndarray::ArrayViewMut2};
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


#[inline]
fn tof_bin_event_linear(tof: i32, start: i32, stop: i32, step: i32) -> Option<usize> {
    if tof < start || tof >= stop {
        return None;
    }
    Some(((tof - start) / step) as usize)
}

/// Accumulates events into a histogram, assuming events are already sorted
/// in time-of-flight. The result is meaningless if events are not sorted.
///
/// This is algorithmically similar to
/// https://github.com/mantidproject/mantid/blob/0d4de52da17df6d055e285902e87ebc53d2de7f6/Framework/DataObjects/src/EventList.cpp#L2575
/// and makes use of the fact that event-lists in ev44s are,
/// in practice, emitted ordered in ToF. This implementation
/// is therefore more efficient than a binary search on bin
/// boundaries for each event - and throughput of this algorithm
/// increases as the number of events in an ev44 increases.
fn accumulate_sorted_events(event_tofs: &[i32], pixel_ids: &[i32], boundaries: &[i32], hist: &mut ArrayViewMut2<f64>) {
    let mut bin_idx = 0;

    for (&tof, &pixel) in event_tofs.iter().zip(pixel_ids) {
        while boundaries.get(bin_idx).map(|&boundary| tof >= boundary).unwrap_or(false) {
            bin_idx += 1;
        }
        if bin_idx >= boundaries.len() {
            // Since events are sorted in ToF, no more events can possibly be added to histogram.
            break;
        }
        if bin_idx >= 1 && let Some(elem) = hist.get_mut((pixel as usize, bin_idx - 1)) {
            *elem += 1.0;
        }
    }
}

/// Accumulates unsorted events into a histogram.
///
/// As above, this is algorithmically similar to
/// https://github.com/mantidproject/mantid/blob/0d4de52da17df6d055e285902e87ebc53d2de7f6/Framework/DataObjects/src/EventList.cpp#L2575
///
/// Sorting events by ToF is more efficient than doing a binary search on each event.
fn accumulate_unsorted_events(event_tofs: &[i32], pixel_ids: &[i32], boundaries: &[i32], hist: &mut ArrayViewMut2<f64>) {
    // Vec<(tof, pixel_id)>
    let mut all_events = event_tofs.iter()
        .zip(pixel_ids.iter())
        .collect::<Vec<_>>();

    all_events.sort_unstable_by_key(|e| e.0);

    let mut bin_idx = 0;

    for (&tof, &pixel) in all_events {
        while boundaries.get(bin_idx).map(|&boundary| tof >= boundary).unwrap_or(false) {
            bin_idx += 1;
        }
        if bin_idx >= boundaries.len() {
            // Since events are sorted in ToF, no more events can possibly be added to histogram.
            break;
        }
        if bin_idx >= 1 && let Some(elem) = hist.get_mut((pixel as usize, bin_idx - 1)) {
            *elem += 1.0;
        }
    }
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
    let mut hist = histogram.as_array_mut();
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

    // In practice, events in ev44 are ordered in ToF already.
    // But ev44 doesn't *guarantee* that. Handle both cases.
    if event_tofs.is_sorted() {
        accumulate_sorted_events(event_tofs, pixel_ids, boundaries, &mut hist)
    } else {
        accumulate_unsorted_events(event_tofs, pixel_ids, boundaries, &mut hist)
    };

    Ok(())
}

/// Bin events into a spectrum array, specified by
/// a (start, stop, step) set of parameters.
///
/// This function does not release the GIL as it mutates
/// a *view* onto a numpy-allocated array. We cannot allow
/// other python threads to concurrently modify (e.g. resize)
/// that array.
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

//     #[test]
//     fn test_tof_bin_event() {
//         let bin_edges = [2, 4, 6];
//         assert_eq!(tof_bin_event(0, &bin_edges), None);
//         assert_eq!(tof_bin_event(1, &bin_edges), None);
//         assert_eq!(tof_bin_event(2, &bin_edges), Some(0));
//         assert_eq!(tof_bin_event(3, &bin_edges), Some(0));
//         assert_eq!(tof_bin_event(4, &bin_edges), Some(1));
//         assert_eq!(tof_bin_event(5, &bin_edges), Some(1));
//         assert_eq!(tof_bin_event(6, &bin_edges), None);
//         assert_eq!(tof_bin_event(7, &bin_edges), None);
//     }

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
