use log::{warn};
use numpy::ndarray::{Array3};
use pyo3::{PyErr, PyResult};
use pyo3::exceptions::PyValueError;

pub struct Histogram {
    data: Array3<f64>,
    bin_boundaries: Vec<i32>,
}

impl Default for Histogram {
    fn default() -> Self {
        Histogram {
            data: Array3::zeros((1, 1, 1)),
            bin_boundaries: vec![0, 100_000_000],
        }
    }
}

impl Histogram {
    pub (crate) fn change_bin_boundaries(&mut self, new_boundaries: Vec<i32>) -> PyResult<()> {
        if new_boundaries.len() < 2 {
            return Err(PyErr::new::<PyValueError, _>("ToF bin boundaries must have length >= 2"));
        }

        if new_boundaries != self.bin_boundaries {
            self.bin_boundaries = new_boundaries;
            self.reset(self.periods(), self.spectra());
        }
        
        Ok(())
    }

    pub (crate) fn reset(&mut self, periods: usize, spectra: usize) {
        self.data = Array3::zeros(
            (periods, spectra, self.bin_boundaries.len()-1)
        );
    }

    pub (crate) fn periods(&self) -> usize {
        self.data.shape()[0]
    }

    pub (crate) fn spectra(&self) -> usize {
        self.data.shape()[1]
    }

    pub (crate) fn time_channels(&self) -> usize {
        self.data.shape()[2]
    }

    pub (crate) fn megabytes(&self) -> f64 {
        (self.data.len() * 8) as f64 / (1024.0 * 1024.0)
    }

    fn accumulate_sorted_events(&mut self, period: usize, tofs: &[i32], pixel_ids: &[i32]) {
        let mut bin_idx = 0;

        for (&tof, &pixel) in tofs.iter().zip(pixel_ids) {
            while self.bin_boundaries
                .get(bin_idx)
                .map(|&boundary| tof >= boundary)
                .unwrap_or(false)
            {
                bin_idx += 1;
            }
            if bin_idx >= self.bin_boundaries.len() {
                // Since events are sorted in ToF, no more events can possibly be added to histogram.
                break;
            }
            if bin_idx >= 1
                && let Some(elem) = self.data.get_mut((period, pixel as usize, bin_idx - 1))
            {
                *elem += 1.0;
            }
        }
    }

    fn accumulate_unsorted_events(
        &mut self, period: usize, tofs: &[i32], pixel_ids: &[i32]
    ) {
        // Vec<(tof, pixel_id)>
        let mut all_events = tofs.iter().zip(pixel_ids.iter()).collect::<Vec<_>>();

        all_events.sort_unstable_by_key(|e| e.0);

        let mut bin_idx = 0;

        for (&tof, &pixel) in all_events {
            while self.bin_boundaries
                .get(bin_idx)
                .map(|&boundary| tof >= boundary)
                .unwrap_or(false)
            {
                bin_idx += 1;
            }
            if bin_idx >= self.bin_boundaries.len() {
                // Since events are sorted in ToF, no more events can possibly be added to histogram.
                break;
            }
            if bin_idx >= 1
                && let Some(elem) = self.data.get_mut((period, pixel as usize, bin_idx - 1))
            {
                *elem += 1.0;
            }
        }
    }

    pub (crate) fn add_events(&mut self, period: usize, tofs: &[i32], pixel_ids: &[i32]) {
        if tofs.len() != pixel_ids.len() {
            warn!("Cannot histogram {} TOFs with {} pixel_ids, ignoring events", tofs.len(), pixel_ids.len());
            return;
        }

        if tofs.is_sorted() {
            self.accumulate_sorted_events(period, tofs, pixel_ids);
        } else {
            self.accumulate_unsorted_events(period, tofs, pixel_ids);
        }
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

        let mut hist = Histogram::default();
        hist.change_bin_boundaries(boundaries.to_vec()).unwrap();

        hist.accumulate_sorted_events(0, &tofs, &pixel_ids);

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

