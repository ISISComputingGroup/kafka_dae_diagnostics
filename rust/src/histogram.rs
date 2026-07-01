use itertools::Itertools;
use log::warn;
use numpy::ndarray::{Array3, ArrayView1, s};
use pyo3::exceptions::PyValueError;
use pyo3::{PyErr, PyResult};

pub struct Histogram {
    data: Array3<f64>,
    bin_boundaries: Vec<i32>,
    bin_centres: Vec<f64>,
}

impl Default for Histogram {
    fn default() -> Self {
        Histogram {
            data: Array3::zeros((1, 1, 1)),
            bin_boundaries: vec![0, 100_000_000],
            bin_centres: vec![50_000_000.0],
        }
    }
}

impl Histogram {
    pub(crate) fn change_parameters(
        &mut self,
        periods: usize,
        spectra: usize,
        new_boundaries: Vec<i32>,
    ) -> PyResult<()> {
        if new_boundaries.len() < 2 {
            return Err(PyErr::new::<PyValueError, _>(
                "ToF bin boundaries must have length >= 2",
            ));
        }

        self.bin_centres = new_boundaries
            .iter()
            .tuples()
            .map(|(&a, &b)| (a + b) as f64 / 2.0)
            .collect();
        self.bin_boundaries = new_boundaries;

        self.data = Array3::zeros((periods, spectra, self.bin_boundaries.len() - 1));

        Ok(())
    }

    pub(crate) fn reset(&mut self, periods: usize, spectra: usize) {
        self.data = Array3::zeros((periods, spectra, self.bin_boundaries.len() - 1));
    }

    pub(crate) fn periods(&self) -> usize {
        self.data.shape()[0]
    }

    pub(crate) fn spectra(&self) -> usize {
        self.data.shape()[1]
    }

    pub(crate) fn time_channels(&self) -> usize {
        self.data.shape()[2]
    }

    pub(crate) fn megabytes(&self) -> f64 {
        (self.data.len() * 8) as f64 / (1024.0 * 1024.0)
    }

    pub(crate) fn bin_boundaries(&self) -> &[i32] {
        &self.bin_boundaries
    }

    pub(crate) fn bin_centres(&self) -> &[f64] {
        &self.bin_centres
    }

    pub(crate) fn data(&self, period: usize, spectrum: usize) -> ArrayView1<'_, f64> {
        self.data.slice(s![period, spectrum, ..])
    }

    fn accumulate_sorted_events(&mut self, period: usize, tofs: &[i32], pixel_ids: &[i32]) {
        let mut bin_idx = 0;

        for (&tof, &pixel) in tofs.iter().zip(pixel_ids) {
            while self
                .bin_boundaries
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

    fn accumulate_unsorted_events(&mut self, period: usize, tofs: &[i32], pixel_ids: &[i32]) {
        // Vec<(tof, pixel_id)>
        let mut all_events = tofs.iter().zip(pixel_ids.iter()).collect::<Vec<_>>();

        all_events.sort_unstable_by_key(|e| e.0);

        let mut bin_idx = 0;

        for (&tof, &pixel) in all_events {
            while self
                .bin_boundaries
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

    pub(crate) fn add_events(&mut self, period: usize, tofs: &[i32], pixel_ids: &[i32]) {
        if tofs.len() != pixel_ids.len() {
            warn!(
                "Cannot histogram {} TOFs with {} pixel_ids, ignoring events",
                tofs.len(),
                pixel_ids.len()
            );
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
    use approx::assert_abs_diff_eq;
    use super::*;
    use numpy::ndarray::Array1;

    #[test]
    fn test_accumulate_sorted_events() {
        let tofs = [10, 20, 30, 30, 40, 50];
        let pixel_ids = [0, 1, 0, 0, 0, 0];
        let boundaries = [15, 25, 35, 45];

        let mut hist = Histogram::default();
        hist.change_parameters(1, 1000, boundaries.to_vec())
            .expect("Failed to change bin boundaries");
        hist.accumulate_sorted_events(0, &tofs, &pixel_ids);

        assert_eq!(hist.data(0, 0), &Array1::from_vec(vec![0., 2., 1.]));
        assert_eq!(hist.data(0, 1), &Array1::from_vec(vec![1., 0., 0.]));
    }

    #[test]
    fn test_accumulate_unsorted_events() {
        let tofs = [10, 30, 50, 40, 30, 20];
        let pixel_ids = [0, 0, 0, 0, 0, 1];
        let boundaries = [15, 25, 35, 45];

        let mut hist = Histogram::default();
        hist.change_parameters(1, 1000, boundaries.to_vec())
            .expect("Failed to change bin boundaries");
        hist.accumulate_unsorted_events(0, &tofs, &pixel_ids);

        assert_eq!(hist.data(0, 0), &Array1::from_vec(vec![0., 2., 1.]));
        assert_eq!(hist.data(0, 1), &Array1::from_vec(vec![1., 0., 0.]));
    }

    #[test]
    fn test_histogram_megabytes() {
        let mut hist = Histogram::default();
        hist.change_parameters(10, 20, vec![1, 2, 3, 4, 5]).unwrap();

        assert_abs_diff_eq!(hist.megabytes(), (10 * 20 * 4 * 8) as f64 / (1024.0 * 1024.0));
    }

    #[test]
    fn test_parameters() {
        let mut hist = Histogram::default();
        hist.change_parameters(10, 20, vec![1, 2, 3, 4, 5]).unwrap();

        assert_eq!(hist.periods(), 10);
        assert_eq!(hist.spectra(), 20);
        assert_eq!(hist.time_channels(), 4);
    }
}
