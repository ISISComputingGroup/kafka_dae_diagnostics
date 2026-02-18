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

Note that when changing rust code, one of the above commands will need to be run after each change to the source code;
python-only changes do not require this.

## Running locally

In a virtual environment, set the following environment variables:

```
set EPICS_PVAS_INTF_ADDR_LIST=127.0.0.1
set PVXS_LOG=*=ERR
```

Then run using:
```
kdaediag --pv-prefix TE:YOURMACHINE:KDAEDIAG: --broker livedata.isis.cclrc.ac.uk:31092 --event-topic YOURMACHINE_events --runinfo-topic YOURMACHINE_runInfo
```

To generate fake event streams for testing, use [`saluki howl`](https://github.com/isisComputingGroup/saluki).

## Benchmarking

Benchmarks are stored in the `benchmarks` folder. Ensure you are using a release build when benchmarking.
