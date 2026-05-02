from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
from trading_framework.core.domain.configuration import CoreConfiguration
from trading_framework.core.domain.state import StrategyState
from trading_framework.core.domain.types import (
    BookLevel,
    BookPayload,
    MarketEvent,
    Price,
    Quantity,
)
from trading_framework.core.events.event_bus import EventBus
from trading_framework.core.risk.risk_config import RiskConfig
from trading_framework.strategies.base import Strategy

import trading_runtime.backtest.engine.strategy_runner as strategy_runner_module
from trading_runtime.backtest.engine.hft_engine import HftEngineConfig
from trading_runtime.backtest.engine.strategy_runner import HftStrategyRunner


class _NoopStrategy(Strategy):
    def on_feed(self, state: Any, event: Any, engine_cfg: Any, constraints: Any) -> list[Any]:
        _ = (state, event, engine_cfg, constraints)
        return []

    def on_order_update(self, state: Any, engine_cfg: Any, constraints: Any) -> list[Any]:
        _ = (state, engine_cfg, constraints)
        return []

    def on_risk_decision(self, decision: Any) -> None:
        _ = decision


class _NoopExecution:
    def apply_intents(self, intents: list[Any]) -> list[tuple[Any, str]]:
        _ = intents
        return []


class _RecorderWrapper:
    recorder: Any

    def __init__(self) -> None:
        self.recorder = SimpleNamespace(record=lambda _hbt: None)


class _StubVenue:
    def __init__(
        self,
        *,
        rc_sequence: list[int],
        ts_sequence: list[int],
        depth: object | None = None,
        state_values: object | None = None,
        orders: object | None = None,
    ) -> None:
        self._rc = list(rc_sequence)
        self._ts = list(ts_sequence)
        self._depth = depth
        self._state_values = state_values
        self._orders = orders
        self._current_ts = 0

    def wait_next(self, *, timeout_ns: int, include_order_resp: bool) -> int:
        _ = (timeout_ns, include_order_resp)
        self._current_ts = self._ts.pop(0)
        return self._rc.pop(0)

    def current_timestamp_ns(self) -> int:
        return self._current_ts

    def read_market_snapshot(self) -> object:
        return self._depth

    def read_orders_snapshot(self) -> tuple[object, object]:
        return self._state_values, self._orders

    def record(self, recorder: Any) -> None:
        recorder.recorder.record(self)


def _core_cfg() -> CoreConfiguration:
    return CoreConfiguration(
        version="v1",
        payload={
            "market": {
                "instruments": {
                    "BTC_USDC-PERPETUAL": {
                        "tick_size": 0.1,
                        "lot_size": 0.01,
                        "contract_size": 1.0,
                    }
                }
            }
        },
    )


def _engine_cfg() -> HftEngineConfig:
    return HftEngineConfig(
        initial_snapshot=None,
        data_files=[],
        instrument="BTC_USDC-PERPETUAL",
        tick_size=0.1,
        lot_size=0.01,
        contract_size=1.0,
        maker_fee_rate=0.0,
        taker_fee_rate=0.0,
        entry_latency_ns=0,
        response_latency_ns=0,
        use_risk_adverse_queue_model=False,
        partial_fill_venue=False,
        max_steps=1,
        last_trades_capacity=1,
        max_price_tick_levels=1,
        roi_lb=0,
        roi_ub=1,
        stats_npz_path="/tmp/stats.npz",
        event_bus_path="/tmp/events.jsonl",
    )


def _risk_cfg() -> RiskConfig:
    return RiskConfig(
        scope="test",
        notional_limits={"currency": "USDC", "max_gross_notional": 1.0},
    )


def _market_event(ts_ns: int) -> MarketEvent:
    return MarketEvent(
        ts_ns_exch=ts_ns,
        ts_ns_local=ts_ns,
        instrument="BTC_USDC-PERPETUAL",
        event_type="book",
        book=BookPayload(
            book_type="snapshot",
            bids=[
                BookLevel(
                    price=Price(currency="UNKNOWN", value=100.0),
                    quantity=Quantity(value=1.0, unit="contracts"),
                )
            ],
            asks=[
                BookLevel(
                    price=Price(currency="UNKNOWN", value=101.0),
                    quantity=Quantity(value=1.0, unit="contracts"),
                )
            ],
            depth=1,
        ),
    )


def _depth_snapshot() -> object:
    return SimpleNamespace(
        roi_lb_tick=100,
        tick_size=0.1,
        best_ask_tick=101,
        best_bid_tick=100,
        ask_depth=[1.0, 0.0],
        bid_depth=[1.0, 0.0],
        best_bid=100.0,
        best_ask=101.0,
        best_bid_qty=1.0,
        best_ask_qty=1.0,
    )


def test_process_market_event_routes_through_event_entry_with_core_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = object.__new__(HftStrategyRunner)
    runner.strategy_state = object()
    runner._core_cfg = _core_cfg()
    runner._next_market_processing_position_index = 0

    captured: list[tuple[int, object]] = []

    def _spy_process_event_entry(state: object, entry: object, *, configuration: object) -> None:
        _ = state
        captured.append((entry.position.index, configuration))

    monkeypatch.setattr(
        strategy_runner_module,
        "process_event_entry",
        _spy_process_event_entry,
    )

    runner._process_canonical_market_event(_market_event(1))
    runner._process_canonical_market_event(_market_event(2))

    assert [idx for idx, _ in captured] == [0, 1]
    assert captured[0][1] is runner._core_cfg
    assert captured[1][1] is runner._core_cfg
    assert runner._next_market_processing_position_index == 2


def test_market_branch_calls_canonical_boundary_not_update_market(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = HftStrategyRunner(
        engine_cfg=_engine_cfg(),
        strategy=_NoopStrategy(),
        risk_cfg=_risk_cfg(),
        core_cfg=_core_cfg(),
    )
    monkeypatch.setattr(
        runner.strategy_state,
        "update_market",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("update_market must not be called")),
    )
    monkeypatch.setattr(
        runner.strategy_state,
        "apply_fill_event",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("apply_fill_event must not be called")),
    )

    captured: list[tuple[int, object]] = []

    def _spy_process_event_entry(state: object, entry: object, *, configuration: object) -> None:
        _ = state
        captured.append((entry.position.index, configuration))

    monkeypatch.setattr(
        strategy_runner_module,
        "process_event_entry",
        _spy_process_event_entry,
    )

    venue = _StubVenue(
        rc_sequence=[0, 2, 1],
        ts_sequence=[1, 2, 3],
        depth=_depth_snapshot(),
    )
    runner.run(venue=venue, execution=_NoopExecution(), recorder=_RecorderWrapper())

    assert captured == [(0, runner._core_cfg)]


def test_missing_core_cfg_fails_before_market_mutation() -> None:
    runner = object.__new__(HftStrategyRunner)
    runner.strategy_state = StrategyState(event_bus=EventBus(sinks=[]))
    runner._core_cfg = None
    runner._next_market_processing_position_index = 0

    with pytest.raises(ValueError, match="CoreConfiguration is required"):
        runner._process_canonical_market_event(_market_event(42))

    assert runner.strategy_state.market == {}
    assert runner.strategy_state._last_processing_position_index is None
    assert runner._next_market_processing_position_index == 0


def test_invalid_core_cfg_type_fails_before_market_mutation() -> None:
    runner = object.__new__(HftStrategyRunner)
    runner.strategy_state = StrategyState(event_bus=EventBus(sinks=[]))
    runner._core_cfg = object()
    runner._next_market_processing_position_index = 0

    with pytest.raises(TypeError, match="configuration must be CoreConfiguration or None"):
        runner._process_canonical_market_event(_market_event(42))

    assert runner.strategy_state.market == {}
    assert runner.strategy_state._last_processing_position_index is None
    assert runner._next_market_processing_position_index == 0


def test_order_snapshot_branch_keeps_compatibility_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = HftStrategyRunner(
        engine_cfg=_engine_cfg(),
        strategy=_NoopStrategy(),
        risk_cfg=_risk_cfg(),
        core_cfg=_core_cfg(),
    )
    monkeypatch.setattr(
        runner.strategy_state,
        "apply_fill_event",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("apply_fill_event must not be called")),
    )

    calls = {"update_account": 0, "ingest_order_snapshots": 0}

    def _spy_update_account(*args: object, **kwargs: object) -> None:
        _ = (args, kwargs)
        calls["update_account"] += 1

    def _spy_ingest_order_snapshots(*args: object, **kwargs: object) -> None:
        _ = (args, kwargs)
        calls["ingest_order_snapshots"] += 1

    monkeypatch.setattr(runner.strategy_state, "update_account", _spy_update_account)
    monkeypatch.setattr(
        runner.strategy_state,
        "ingest_order_snapshots",
        _spy_ingest_order_snapshots,
    )

    venue = _StubVenue(
        rc_sequence=[0, 3, 1],
        ts_sequence=[1, 2, 3],
        state_values=SimpleNamespace(
            position=0.0,
            balance=1000.0,
            fee=0.0,
            trading_volume=0.0,
            trading_value=0.0,
            num_trades=0,
        ),
        orders={},
    )

    runner.run(venue=venue, execution=_NoopExecution(), recorder=_RecorderWrapper())

    assert calls["update_account"] == 1
    assert calls["ingest_order_snapshots"] == 1
