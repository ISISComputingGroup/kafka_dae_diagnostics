# Local development

This project {doc}`is a Python project with a Rust extension module <architectural_decision_records/002_python_rust>`. It uses the
[PyO3 Rust/Python bindings](https://pyo3.rs/) and [Maturin build system](https://www.maturin.rs/index.html).

To develop, you will need a Rust toolchain installed. The project can then be installed like any usual python
project; run the following command in a `venv` managed by `uv`:

```
uv pip install -e .[dev]
```

You can also use `maturin` directly once installed:

```shell
maturin develop  # debug build
maturin develop --release  # release build
```
