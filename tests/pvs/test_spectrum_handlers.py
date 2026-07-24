import pytest

from kafka_dae_diagnostics.data import Data
from kafka_dae_diagnostics.pvs.spectrum_handlers import SpectrumHandler

PREFIX = "UNITTEST:"


@pytest.fixture
def data():
    return Data()


@pytest.fixture
def handler(data: Data):
    return SpectrumHandler(
        prefix=PREFIX,
        data=data,
    )


def test_spectrum_handler_test_channel(handler: SpectrumHandler):
    assert handler.testChannel("UNITTEST:SPEC:0:0:X")
    assert handler.testChannel("UNITTEST:SPEC:0:0:Y")
    assert handler.testChannel("UNITTEST:SPEC:0:0:XE")
    assert handler.testChannel("UNITTEST:SPEC:0:0:YC")
    assert not handler.testChannel("UNITTEST:SPEC:0:0:")
    assert not handler.testChannel("UNITTEST:SPEC:0:0:Y2")
    assert not handler.testChannel("BLAHBLAH:SPEC:0:0:Y")
    assert not handler.testChannel("BLAHBLAH:SPEC:0:0:Y")
    assert not handler.testChannel("some_random_pv")


@pytest.mark.parametrize(
    "pvname",
    ["UNITTEST:SPEC:0:0:X", "UNITTEST:SPEC:0:0:Y", "UNITTEST:SPEC:1:0:XE", "UNITTEST:SPEC:1:0:YC"],
)
def test_spectrum_handler_make_channel(handler: SpectrumHandler, pvname: str, data: Data):
    assert len(data.callbacks) == 0
    pv = handler.makeChannel(pvname, "localhost")
    assert len(data.callbacks) == 1
    pv._handler.onLastDisconnect()  # type: ignore
    assert len(data.callbacks) == 0


def test_spectrum_handler_make_invalid_channel(handler: SpectrumHandler):
    # p4p should never allow this to happen...
    with pytest.raises(
        AssertionError, match=r"No match in makeChannel after there was a match in testChannel?"
    ):
        handler.makeChannel("UNITTEST:SPEC:0:0:BLAH", "localhost")
