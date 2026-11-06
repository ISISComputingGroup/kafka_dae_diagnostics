import logging
from pathlib import Path

import pytest

from kafka_dae_diagnostics.data import Data
from kafka_dae_diagnostics.save_restore import load_from_file, save_to_file


def test_save_load_data(tmp_path: Path) -> None:
    state_file = tmp_path / "state.json"

    data = Data(
        linear_tcb_start_ns=123,
        linear_tcb_end_ns=456,
        linear_tcb_num=789,
    )
    save_to_file(data, state_file)

    loaded = load_from_file(state_file)
    assert loaded == {
        "linear_tcb_start_ns": 123,
        "linear_tcb_end_ns": 456,
        "linear_tcb_num": 789,
    }


def test_save_data_exception(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    state_file = tmp_path / "nonexistent.json"

    with caplog.at_level(logging.WARNING, logger="kafka_dae_diagnostics"):
        save_to_file(
            Data(
                linear_tcb_start_ns=object(),  # pyright: ignore - intentionally wrong type, unserializable
            ),
            state_file,
        )

    assert "Failed to save to state file" in caplog.text


def test_load_data_exception(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    state_file = tmp_path / "nonexistent.json"

    with caplog.at_level(logging.WARNING, logger="kafka_dae_diagnostics"):
        assert load_from_file(state_file) == {}

    assert "Failed to load state file" in caplog.text
