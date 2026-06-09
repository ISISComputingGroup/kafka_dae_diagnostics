mod data;
mod handlers;

use numpy::{PyReadonlyArray1, PyReadwriteArray2, ndarray::ArrayViewMut2};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

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
fn accumulate_sorted_events(
    event_tofs: &[i32],
    pixel_ids: &[i32],
    boundaries: &[i32],
    hist: &mut ArrayViewMut2<f64>,
) {
    let mut bin_idx = 0;

    for (&tof, &pixel) in event_tofs.iter().zip(pixel_ids) {
        while boundaries
            .get(bin_idx)
            .map(|&boundary| tof >= boundary)
            .unwrap_or(false)
        {
            bin_idx += 1;
        }
        if bin_idx >= boundaries.len() {
            // Since events are sorted in ToF, no more events can possibly be added to histogram.
            break;
        }
        if bin_idx >= 1
            && let Some(elem) = hist.get_mut((pixel as usize, bin_idx - 1))
        {
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
fn accumulate_unsorted_events(
    event_tofs: &[i32],
    pixel_ids: &[i32],
    boundaries: &[i32],
    hist: &mut ArrayViewMut2<f64>,
) {
    // Vec<(tof, pixel_id)>
    let mut all_events = event_tofs.iter().zip(pixel_ids.iter()).collect::<Vec<_>>();

    all_events.sort_unstable_by_key(|e| e.0);

    let mut bin_idx = 0;

    for (&tof, &pixel) in all_events {
        while boundaries
            .get(bin_idx)
            .map(|&boundary| tof >= boundary)
            .unwrap_or(false)
        {
            bin_idx += 1;
        }
        if bin_idx >= boundaries.len() {
            // Since events are sorted in ToF, no more events can possibly be added to histogram.
            break;
        }
        if bin_idx >= 1
            && let Some(elem) = hist.get_mut((pixel as usize, bin_idx - 1))
        {
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

#[pymodule]
mod _kdaediag_rs {
    use pyo3::prelude::*;

    #[pymodule_export]
    use super::bin_events_into_spectrum;

    #[pymodule_export]
    use super::data::Data;

    #[pymodule_init]
    fn init(m: &Bound<'_, PyModule>) -> PyResult<()> {
        pyo3_log::init();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use numpy::ndarray::Array2;

    #[test]
    fn test_accumulate_sorted_events() {
        let tofs = [10, 20, 30, 30, 40, 50];
        let pixel_ids = [0, 1, 0, 0, 0, 0];
        let boundaries = [15, 25, 35, 45];

        let mut hist = Array2::zeros((2, 3));

        accumulate_sorted_events(&tofs, &pixel_ids, &boundaries, &mut hist.view_mut());

        assert_eq!(hist[(0, 0)], 0.0);
        assert_eq!(hist[(0, 1)], 2.0);
        assert_eq!(hist[(0, 2)], 1.0);
        assert_eq!(hist[(1, 0)], 1.0);
    }

    #[test]
    fn test_accumulate_unsorted_events() {
        let tofs = [10, 30, 50, 40, 30, 20];
        let pixel_ids = [0, 0, 0, 0, 0, 1];
        let boundaries = [15, 25, 35, 45];

        let mut hist = Array2::zeros((2, 3));

        accumulate_unsorted_events(&tofs, &pixel_ids, &boundaries, &mut hist.view_mut());

        assert_eq!(hist[(0, 0)], 0.0);
        assert_eq!(hist[(0, 1)], 2.0);
        assert_eq!(hist[(0, 2)], 1.0);
        assert_eq!(hist[(1, 0)], 1.0);
    }
}
