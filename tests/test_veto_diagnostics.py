import numpy as np
import pytest

from kafka_dae_diagnostics.veto_diagnostics import VetoDiagnostics


@pytest.fixture
def veto_diagnostics() -> VetoDiagnostics:
    return VetoDiagnostics(max_recent_frames=100)


def test_no_vetos(veto_diagnostics: VetoDiagnostics) -> None:
    assert np.all(veto_diagnostics.get_run_veto_count() == 0)
    assert np.all(veto_diagnostics.get_recent_veto_count() == 0)

    np.testing.assert_allclose(veto_diagnostics.get_run_veto_percentages(), 0.0)
    np.testing.assert_allclose(veto_diagnostics.get_recent_veto_percentages(), 0.0)


def test_one_veto(veto_diagnostics: VetoDiagnostics) -> None:
    veto_diagnostics.add_veto(0b11010011)

    assert np.all(
        veto_diagnostics.get_run_veto_count() == np.array([1, 1, 0, 0, 1, 0, 1, 1] + [0] * 24)
    )
    assert np.all(
        veto_diagnostics.get_recent_veto_count() == np.array([1, 1, 0, 0, 1, 0, 1, 1] + [0] * 24)
    )

    np.testing.assert_allclose(
        veto_diagnostics.get_run_veto_percentages(),
        np.array([100, 100, 0, 0, 100, 0, 100, 100] + [0] * 24),
    )
    np.testing.assert_allclose(
        veto_diagnostics.get_recent_veto_percentages(),
        np.array([100, 100, 0, 0, 100, 0, 100, 100] + [0] * 24),
    )


def test_two_different_vetos(veto_diagnostics: VetoDiagnostics) -> None:
    veto_diagnostics.add_veto(0b11010011)
    veto_diagnostics.add_veto(0)

    assert np.all(
        veto_diagnostics.get_run_veto_count() == np.array([1, 1, 0, 0, 1, 0, 1, 1] + [0] * 24)
    )
    assert np.all(
        veto_diagnostics.get_recent_veto_count() == np.array([1, 1, 0, 0, 1, 0, 1, 1] + [0] * 24)
    )

    np.testing.assert_allclose(
        veto_diagnostics.get_run_veto_percentages(),
        np.array([50, 50, 0, 0, 50, 0, 50, 50] + [0] * 24),
    )
    np.testing.assert_allclose(
        veto_diagnostics.get_recent_veto_percentages(),
        np.array([50, 50, 0, 0, 50, 0, 50, 50] + [0] * 24),
    )


def test_many_vetos(veto_diagnostics: VetoDiagnostics) -> None:

    num_vetos = 123456

    for _ in range(num_vetos):
        veto_diagnostics.add_veto(0b11010011)

    assert np.all(
        veto_diagnostics.get_run_veto_count()
        == np.array([num_vetos, num_vetos, 0, 0, num_vetos, 0, num_vetos, num_vetos] + [0] * 24)
    )
    assert np.all(
        veto_diagnostics.get_recent_veto_count()
        == np.array([100, 100, 0, 0, 100, 0, 100, 100] + [0] * 24)
    )

    np.testing.assert_allclose(
        veto_diagnostics.get_run_veto_percentages(),
        np.array([100, 100, 0, 0, 100, 0, 100, 100] + [0] * 24),
    )
    np.testing.assert_allclose(
        veto_diagnostics.get_recent_veto_percentages(),
        np.array([100, 100, 0, 0, 100, 0, 100, 100] + [0] * 24),
    )


def test_reset(veto_diagnostics: VetoDiagnostics) -> None:
    veto_diagnostics.add_veto(0b11010011)

    assert veto_diagnostics.get_run_veto_percentages()[0] == pytest.approx(100)
    assert veto_diagnostics.get_recent_veto_percentages()[0] == pytest.approx(100)
    assert veto_diagnostics.get_run_veto_count()[0] == 1
    assert veto_diagnostics.get_recent_veto_count()[0] == 1

    veto_diagnostics.reset()

    assert veto_diagnostics.get_run_veto_percentages()[0] == pytest.approx(0)
    assert veto_diagnostics.get_recent_veto_percentages()[0] == pytest.approx(0)
    assert veto_diagnostics.get_run_veto_count()[0] == 0
    assert veto_diagnostics.get_recent_veto_count()[0] == 0
