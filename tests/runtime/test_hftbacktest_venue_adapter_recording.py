from __future__ import annotations

from types import SimpleNamespace

import pytest

from core_runtime.backtest.adapters.venue import HftBacktestVenueAdapter


def test_record_swallows_expected_recorder_index_error() -> None:
    calls: list[object] = []

    def _record(hbt: object) -> None:
        calls.append(hbt)
        raise IndexError

    hbt = object()
    recorder = SimpleNamespace(recorder=SimpleNamespace(record=_record))
    venue = HftBacktestVenueAdapter(hbt=hbt, asset_no=0)  # type: ignore[arg-type]

    venue.record(recorder)

    assert calls == [hbt]


def test_record_propagates_unexpected_recorder_exceptions() -> None:
    def _record(_hbt: object) -> None:
        raise ValueError("unexpected recorder failure")

    recorder = SimpleNamespace(recorder=SimpleNamespace(record=_record))
    venue = HftBacktestVenueAdapter(hbt=object(), asset_no=0)  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="unexpected recorder failure"):
        venue.record(recorder)
