mod diag_data;
mod frame_metadata;
mod histogram;

use pyo3::prelude::*;

#[pymodule]
mod _kdaediag_rs {
    use pyo3::prelude::*;

    #[pymodule_export]
    use super::diag_data::Data;

    #[pymodule_init]
    fn init(_: &Bound<'_, PyModule>) -> PyResult<()> {
        pyo3_log::init();
        Ok(())
    }
}
