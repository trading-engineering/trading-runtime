from __future__ import annotations

from collections import deque
from types import SimpleNamespace
from typing import Any

import pytest
from trading_framework.core.domain.configuration import CoreConfiguration
from trading_framework.core.domain.state import StrategyState
from trading_framework.core.domain.types import (
    BookLevel,
    BookPayload,
    CancelOrderIntent,
    ControlTimeEvent,
    MarketEvent,
    NewOrderIntent,
    OrderSubmittedEvent,
    Price,
    Quantity,
    ReplaceOrderIntent,
)
from trading_framework.core.events.event_bus import EventBus
from trading_framework.core.risk.risk_config import RiskConfig
from trading_framework.core.risk.risk_engine import GateDecision
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


def _new_intent(ts_ns_local: int = 2) -> NewOrderIntent:
    return NewOrderIntent(
        ts_ns_local=ts_ns_local,
        instrument="BTC_USDC-PERPETUAL",
        client_order_id="cid-new-1",
        intents_correlation_id="corr-new-1",
        side="buy",
        order_type="limit",
        intended_qty=Quantity(value=1.0, unit="contracts"),
        intended_price=Price(currency="USDC", value=100.0),
        time_in_force="GTC",
    )


def _replace_intent(ts_ns_local: int = 2) -> ReplaceOrderIntent:
    return ReplaceOrderIntent(
        ts_ns_local=ts_ns_local,
        instrument="BTC_USDC-PERPETUAL",
        client_order_id="cid-existing-1",
        intents_correlation_id="corr-replace-1",
        side="buy",
        order_type="limit",
        intended_qty=Quantity(value=2.0, unit="contracts"),
        intended_price=Price(currency="USDC", value=101.0),
    )


def _cancel_intent(ts_ns_local: int = 2) -> CancelOrderIntent:
    return CancelOrderIntent(
        ts_ns_local=ts_ns_local,
        instrument="BTC_USDC-PERPETUAL",
        client_order_id="cid-existing-1",
        intents_correlation_id="corr-cancel-1",
    )


class _EmitIntentsStrategy(Strategy):
    def __init__(self, intents: list[object]) -> None:
        self._intents = intents

    def on_feed(self, state: Any, event: Any, engine_cfg: Any, constraints: Any) -> list[Any]:
        _ = (state, event, engine_cfg, constraints)
        return list(self._intents)

    def on_order_update(self, state: Any, engine_cfg: Any, constraints: Any) -> list[Any]:
        _ = (state, engine_cfg, constraints)
        return []

    def on_risk_decision(self, decision: Any) -> None:
        _ = decision


def _decision_for(accepted_now: list[Any]) -> GateDecision:
    return GateDecision(
        ts_ns_local=2,
        accepted_now=accepted_now,
        queued=[],
        rejected=[],
        replaced_in_queue=[],
        dropped_in_queue=[],
        handled_in_queue=[],
        execution_rejected=[],
        next_send_ts_ns_local=None,
    )


def test_process_market_event_routes_through_event_entry_with_core_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = object.__new__(HftStrategyRunner)
    runner.strategy_state = object()
    runner._core_cfg = _core_cfg()
    runner._next_canonical_processing_position_index = 0

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
    assert runner._next_canonical_processing_position_index == 2


def test_first_canonical_event_uses_processing_position_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = object.__new__(HftStrategyRunner)
    runner.strategy_state = object()
    runner._core_cfg = _core_cfg()
    runner._next_canonical_processing_position_index = 0

    captured: list[int] = []

    def _spy_process_event_entry(state: object, entry: object, *, configuration: object) -> None:
        _ = state
        assert configuration is runner._core_cfg
        captured.append(entry.position.index)

    monkeypatch.setattr(
        strategy_runner_module,
        "process_event_entry",
        _spy_process_event_entry,
    )

    runner._process_canonical_market_event(_market_event(1))

    assert captured == [0]
    assert runner._next_canonical_processing_position_index == 1


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
    runner._next_canonical_processing_position_index = 0

    with pytest.raises(ValueError, match="CoreConfiguration is required"):
        runner._process_canonical_market_event(_market_event(42))

    assert runner.strategy_state.market == {}
    assert runner.strategy_state._last_processing_position_index is None
    assert runner._next_canonical_processing_position_index == 0


def test_invalid_core_cfg_type_fails_before_market_mutation() -> None:
    runner = object.__new__(HftStrategyRunner)
    runner.strategy_state = StrategyState(event_bus=EventBus(sinks=[]))
    runner._core_cfg = object()
    runner._next_canonical_processing_position_index = 0

    with pytest.raises(TypeError, match="configuration must be CoreConfiguration or None"):
        runner._process_canonical_market_event(_market_event(42))

    assert runner.strategy_state.market == {}
    assert runner.strategy_state._last_processing_position_index is None
    assert runner._next_canonical_processing_position_index == 0


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
    assert runner._next_canonical_processing_position_index == 0


def test_snapshot_only_rc3_does_not_consume_canonical_cursor_position(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = HftStrategyRunner(
        engine_cfg=_engine_cfg(),
        strategy=_NoopStrategy(),
        risk_cfg=_risk_cfg(),
        core_cfg=_core_cfg(),
    )

    calls = {"update_account": 0, "ingest_order_snapshots": 0, "canonical": 0}

    def _spy_update_account(*args: object, **kwargs: object) -> None:
        _ = (args, kwargs)
        calls["update_account"] += 1

    def _spy_ingest_order_snapshots(*args: object, **kwargs: object) -> None:
        _ = (args, kwargs)
        calls["ingest_order_snapshots"] += 1

    def _spy_process_event_entry(*args: object, **kwargs: object) -> None:
        _ = (args, kwargs)
        calls["canonical"] += 1

    monkeypatch.setattr(runner.strategy_state, "update_account", _spy_update_account)
    monkeypatch.setattr(
        runner.strategy_state,
        "ingest_order_snapshots",
        _spy_ingest_order_snapshots,
    )
    monkeypatch.setattr(
        strategy_runner_module,
        "process_event_entry",
        _spy_process_event_entry,
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

    assert calls == {
        "update_account": 1,
        "ingest_order_snapshots": 1,
        "canonical": 0,
    }
    assert runner._next_canonical_processing_position_index == 0


def test_successful_new_dispatch_processes_order_submitted_before_mark_sent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    new_intent = _new_intent()
    runner = HftStrategyRunner(
        engine_cfg=_engine_cfg(),
        strategy=_EmitIntentsStrategy([new_intent]),
        risk_cfg=_risk_cfg(),
        core_cfg=_core_cfg(),
    )
    monkeypatch.setattr(runner.strategy_state, "apply_fill_event", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        runner.risk,
        "decide_intents",
        lambda **_: _decision_for([new_intent]),
    )

    ordering: list[str] = []
    submitted_events: list[OrderSubmittedEvent] = []
    marks: list[tuple[str, str, str]] = []

    def _spy_process_event_entry(state: object, entry: object, *, configuration: object) -> None:
        _ = (state, configuration)
        if isinstance(entry.event, OrderSubmittedEvent):
            ordering.append("submitted")
            submitted_events.append(entry.event)

    def _spy_mark_intent_sent(instrument: str, client_order_id: str, intent_type: str) -> None:
        ordering.append("mark")
        marks.append((instrument, client_order_id, intent_type))

    monkeypatch.setattr(strategy_runner_module, "process_event_entry", _spy_process_event_entry)
    monkeypatch.setattr(runner.strategy_state, "mark_intent_sent", _spy_mark_intent_sent)

    venue = _StubVenue(
        rc_sequence=[0, 2, 1],
        ts_sequence=[1_111, 5_000_000_000, 5_000_000_001],
        depth=_depth_snapshot(),
    )
    runner.run(venue=venue, execution=_NoopExecution(), recorder=_RecorderWrapper())

    assert len(submitted_events) == 1
    event = submitted_events[0]
    assert event.instrument == new_intent.instrument
    assert event.client_order_id == new_intent.client_order_id
    assert event.side == new_intent.side
    assert event.order_type == new_intent.order_type
    assert event.intended_price == new_intent.intended_price
    assert event.intended_qty == new_intent.intended_qty
    assert event.time_in_force == new_intent.time_in_force
    assert event.intent_correlation_id == new_intent.intents_correlation_id
    assert event.dispatch_attempt_id is None
    assert event.runtime_correlation is None
    assert event.ts_ns_local_dispatch == 5_000_000_000
    assert ordering == ["submitted", "mark"]
    assert marks == [(new_intent.instrument, new_intent.client_order_id, "new")]


def test_failed_new_dispatch_processes_no_order_submitted_event(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    new_intent = _new_intent()
    runner = HftStrategyRunner(
        engine_cfg=_engine_cfg(),
        strategy=_EmitIntentsStrategy([new_intent]),
        risk_cfg=_risk_cfg(),
        core_cfg=_core_cfg(),
    )
    monkeypatch.setattr(
        runner.risk,
        "decide_intents",
        lambda **_: _decision_for([new_intent]),
    )

    submitted_event_count = 0
    marked_count = 0
    captured_decisions: list[GateDecision] = []

    def _spy_process_event_entry(state: object, entry: object, *, configuration: object) -> None:
        nonlocal submitted_event_count
        _ = (state, configuration)
        if isinstance(entry.event, OrderSubmittedEvent):
            submitted_event_count += 1

    def _spy_mark_intent_sent(instrument: str, client_order_id: str, intent_type: str) -> None:
        nonlocal marked_count
        _ = (instrument, client_order_id, intent_type)
        marked_count += 1

    monkeypatch.setattr(strategy_runner_module, "process_event_entry", _spy_process_event_entry)
    monkeypatch.setattr(runner.strategy_state, "mark_intent_sent", _spy_mark_intent_sent)
    monkeypatch.setattr(
        runner.strategy,
        "on_risk_decision",
        lambda decision: captured_decisions.append(decision),
    )

    class _ExecutionFailNew:
        def apply_intents(self, intents: list[Any]) -> list[tuple[Any, str]]:
            _ = intents
            return [(new_intent, "EXCHANGE_REJECT")]

    venue = _StubVenue(
        rc_sequence=[0, 2, 1],
        ts_sequence=[10, 20, 30],
        depth=_depth_snapshot(),
    )
    runner.run(
        venue=venue,
        execution=_ExecutionFailNew(),
        recorder=_RecorderWrapper(),
    )

    assert submitted_event_count == 0
    assert marked_count == 0
    assert len(captured_decisions) == 1
    assert len(captured_decisions[0].execution_rejected) == 1
    assert captured_decisions[0].execution_rejected[0].intent.client_order_id == new_intent.client_order_id


def test_successful_replace_cancel_dispatch_processes_no_order_submitted_event(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    replace_intent = _replace_intent()
    cancel_intent = _cancel_intent()
    accepted_now = [replace_intent, cancel_intent]

    runner = HftStrategyRunner(
        engine_cfg=_engine_cfg(),
        strategy=_EmitIntentsStrategy(accepted_now),
        risk_cfg=_risk_cfg(),
        core_cfg=_core_cfg(),
    )
    monkeypatch.setattr(
        runner.risk,
        "decide_intents",
        lambda **_: _decision_for(accepted_now),
    )

    submitted_event_count = 0
    marks: list[tuple[str, str, str]] = []

    def _spy_process_event_entry(state: object, entry: object, *, configuration: object) -> None:
        nonlocal submitted_event_count
        _ = (state, configuration)
        if isinstance(entry.event, OrderSubmittedEvent):
            submitted_event_count += 1

    def _spy_mark_intent_sent(instrument: str, client_order_id: str, intent_type: str) -> None:
        marks.append((instrument, client_order_id, intent_type))

    monkeypatch.setattr(strategy_runner_module, "process_event_entry", _spy_process_event_entry)
    monkeypatch.setattr(runner.strategy_state, "mark_intent_sent", _spy_mark_intent_sent)

    venue = _StubVenue(
        rc_sequence=[0, 2, 1],
        ts_sequence=[100, 200, 300],
        depth=_depth_snapshot(),
    )
    runner.run(venue=venue, execution=_NoopExecution(), recorder=_RecorderWrapper())

    assert submitted_event_count == 0
    assert marks == [
        (
            replace_intent.instrument,
            replace_intent.client_order_id,
            "replace",
        ),
        (
            cancel_intent.instrument,
            cancel_intent.client_order_id,
            "cancel",
        ),
    ]


def test_global_canonical_counter_shared_between_market_and_order_submitted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    new_intent = _new_intent()
    runner = HftStrategyRunner(
        engine_cfg=_engine_cfg(),
        strategy=_EmitIntentsStrategy([new_intent]),
        risk_cfg=_risk_cfg(),
        core_cfg=_core_cfg(),
    )
    monkeypatch.setattr(
        runner.risk,
        "decide_intents",
        lambda **_: _decision_for([new_intent]),
    )

    positions: list[tuple[int, str]] = []

    def _spy_process_event_entry(state: object, entry: object, *, configuration: object) -> None:
        _ = (state, configuration)
        event_name = type(entry.event).__name__
        positions.append((entry.position.index, event_name))

    monkeypatch.setattr(strategy_runner_module, "process_event_entry", _spy_process_event_entry)

    venue = _StubVenue(
        rc_sequence=[0, 2, 1],
        ts_sequence=[7, 9_999_999_999, 10_000_000_000],
        depth=_depth_snapshot(),
    )
    runner.run(venue=venue, execution=_NoopExecution(), recorder=_RecorderWrapper())

    assert positions == [
        (0, "MarketEvent"),
        (1, "OrderSubmittedEvent"),
    ]
    assert runner._next_canonical_processing_position_index == 2


def test_canonical_counter_increments_only_after_successful_canonical_processing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = object.__new__(HftStrategyRunner)
    runner.strategy_state = object()
    runner._core_cfg = _core_cfg()
    runner._next_canonical_processing_position_index = 0

    def _fail(*args: object, **kwargs: object) -> None:
        _ = (args, kwargs)
        raise RuntimeError("boom")

    monkeypatch.setattr(strategy_runner_module, "process_event_entry", _fail)
    with pytest.raises(RuntimeError, match="boom"):
        runner._process_canonical_market_event(_market_event(1))
    assert runner._next_canonical_processing_position_index == 0

    called = {"count": 0}

    def _ok(*args: object, **kwargs: object) -> None:
        _ = (args, kwargs)
        called["count"] += 1

    monkeypatch.setattr(strategy_runner_module, "process_event_entry", _ok)
    runner._process_canonical_market_event(_market_event(2))
    assert called["count"] == 1
    assert runner._next_canonical_processing_position_index == 1


def test_control_time_event_injected_when_scheduled_deadline_is_realized(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = HftStrategyRunner(
        engine_cfg=_engine_cfg(),
        strategy=_NoopStrategy(),
        risk_cfg=_risk_cfg(),
        core_cfg=_core_cfg(),
    )
    runner._next_send_ts_ns_local = 5

    control_events: list[ControlTimeEvent] = []

    def _spy_process_event_entry(state: object, entry: object, *, configuration: object) -> None:
        _ = (state, configuration)
        if isinstance(entry.event, ControlTimeEvent):
            control_events.append(entry.event)

    monkeypatch.setattr(strategy_runner_module, "process_event_entry", _spy_process_event_entry)
    venue = _StubVenue(
        rc_sequence=[0, 0, 1],
        ts_sequence=[1, 10, 11],
    )
    runner.run(venue=venue, execution=_NoopExecution(), recorder=_RecorderWrapper())

    assert len(control_events) == 1
    event = control_events[0]
    assert event.ts_ns_local_control == 10
    assert event.reason == "scheduled_control_recheck"
    assert event.due_ts_ns_local == 5
    assert event.realized_ts_ns_local == 10
    assert event.obligation_reason == "rate_limit"
    assert event.obligation_due_ts_ns_local == 5
    assert event.runtime_correlation is None


def test_no_control_time_event_when_no_deadline_scheduled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = HftStrategyRunner(
        engine_cfg=_engine_cfg(),
        strategy=_NoopStrategy(),
        risk_cfg=_risk_cfg(),
        core_cfg=_core_cfg(),
    )
    control_count = 0

    def _spy_process_event_entry(state: object, entry: object, *, configuration: object) -> None:
        nonlocal control_count
        _ = (state, configuration)
        if isinstance(entry.event, ControlTimeEvent):
            control_count += 1

    monkeypatch.setattr(strategy_runner_module, "process_event_entry", _spy_process_event_entry)
    venue = _StubVenue(
        rc_sequence=[0, 2, 1],
        ts_sequence=[1, 2, 3],
        depth=_depth_snapshot(),
    )
    runner.run(venue=venue, execution=_NoopExecution(), recorder=_RecorderWrapper())

    assert control_count == 0


def test_no_control_time_event_when_deadline_not_yet_realized(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = HftStrategyRunner(
        engine_cfg=_engine_cfg(),
        strategy=_NoopStrategy(),
        risk_cfg=_risk_cfg(),
        core_cfg=_core_cfg(),
    )
    runner._next_send_ts_ns_local = 50
    control_count = 0

    def _spy_process_event_entry(state: object, entry: object, *, configuration: object) -> None:
        nonlocal control_count
        _ = (state, configuration)
        if isinstance(entry.event, ControlTimeEvent):
            control_count += 1

    monkeypatch.setattr(strategy_runner_module, "process_event_entry", _spy_process_event_entry)
    venue = _StubVenue(
        rc_sequence=[0, 0, 1],
        ts_sequence=[1, 10, 20],
    )
    runner.run(venue=venue, execution=_NoopExecution(), recorder=_RecorderWrapper())

    assert control_count == 0


def test_control_time_deadline_injection_is_not_periodic_for_same_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = HftStrategyRunner(
        engine_cfg=_engine_cfg(),
        strategy=_NoopStrategy(),
        risk_cfg=_risk_cfg(),
        core_cfg=_core_cfg(),
    )
    runner._next_send_ts_ns_local = 5
    control_count = 0

    def _spy_process_event_entry(state: object, entry: object, *, configuration: object) -> None:
        nonlocal control_count
        _ = (state, configuration)
        if isinstance(entry.event, ControlTimeEvent):
            control_count += 1

    monkeypatch.setattr(strategy_runner_module, "process_event_entry", _spy_process_event_entry)
    venue = _StubVenue(
        rc_sequence=[0, 0, 0, 1],
        ts_sequence=[1, 10, 10, 11],
    )
    runner.run(venue=venue, execution=_NoopExecution(), recorder=_RecorderWrapper())

    assert control_count == 1


def test_control_time_event_processed_after_pop_and_before_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queued_intent = _new_intent()
    runner = HftStrategyRunner(
        engine_cfg=_engine_cfg(),
        strategy=_NoopStrategy(),
        risk_cfg=_risk_cfg(),
        core_cfg=_core_cfg(),
    )
    runner._next_send_ts_ns_local = 5

    ordering: list[str] = []
    captured_raw_inputs: list[list[Any]] = []

    def _spy_pop_queued_intents(instrument: str) -> list[Any]:
        _ = instrument
        ordering.append("pop")
        return [queued_intent]

    def _spy_process_event_entry(state: object, entry: object, *, configuration: object) -> None:
        _ = (state, configuration)
        if isinstance(entry.event, ControlTimeEvent):
            ordering.append("control")

    def _spy_decide_intents(**kwargs: Any) -> GateDecision:
        ordering.append("gate")
        captured_raw_inputs.append(list(kwargs["raw_intents"]))
        return _decision_for([])

    monkeypatch.setattr(runner.strategy_state, "pop_queued_intents", _spy_pop_queued_intents)
    monkeypatch.setattr(strategy_runner_module, "process_event_entry", _spy_process_event_entry)
    monkeypatch.setattr(runner.risk, "decide_intents", _spy_decide_intents)

    venue = _StubVenue(
        rc_sequence=[0, 0, 1],
        ts_sequence=[1, 10, 11],
    )
    runner.run(venue=venue, execution=_NoopExecution(), recorder=_RecorderWrapper())

    assert ordering == ["pop", "control", "gate"]
    assert len(captured_raw_inputs) == 1
    assert [it.client_order_id for it in captured_raw_inputs[0]] == [queued_intent.client_order_id]


def test_global_canonical_counter_shared_with_control_time_market_and_submitted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    new_intent = _new_intent()
    runner = HftStrategyRunner(
        engine_cfg=_engine_cfg(),
        strategy=_EmitIntentsStrategy([new_intent]),
        risk_cfg=_risk_cfg(),
        core_cfg=_core_cfg(),
    )
    runner._next_send_ts_ns_local = 5

    monkeypatch.setattr(
        runner.risk,
        "decide_intents",
        lambda **_: _decision_for([new_intent]),
    )

    positions: list[tuple[int, str]] = []

    def _spy_process_event_entry(state: object, entry: object, *, configuration: object) -> None:
        _ = (state, configuration)
        positions.append((entry.position.index, type(entry.event).__name__))

    monkeypatch.setattr(strategy_runner_module, "process_event_entry", _spy_process_event_entry)
    venue = _StubVenue(
        rc_sequence=[0, 2, 1],
        ts_sequence=[1, 10, 11],
        depth=_depth_snapshot(),
    )
    runner.run(venue=venue, execution=_NoopExecution(), recorder=_RecorderWrapper())

    assert positions == [
        (0, "MarketEvent"),
        (1, "ControlTimeEvent"),
        (2, "OrderSubmittedEvent"),
    ]
    assert runner._next_canonical_processing_position_index == 3


def test_fallback_second_boundary_wakeup_behavior_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    intent = _new_intent()
    runner = HftStrategyRunner(
        engine_cfg=_engine_cfg(),
        strategy=_EmitIntentsStrategy([intent]),
        risk_cfg=_risk_cfg(),
        core_cfg=_core_cfg(),
    )
    runner.strategy_state.queued_intents.setdefault(runner.engine_cfg.instrument, deque())
    runner.strategy_state.queued_intents[runner.engine_cfg.instrument].append(
        SimpleNamespace(intent=intent)
    )

    monkeypatch.setattr(runner.risk, "decide_intents", lambda **_: _decision_for([]))

    venue = _StubVenue(
        rc_sequence=[0, 2, 1],
        ts_sequence=[1, 2_000_000_000, 2_000_000_001],
        depth=_depth_snapshot(),
    )
    runner.run(venue=venue, execution=_NoopExecution(), recorder=_RecorderWrapper())

    assert runner._next_send_ts_ns_local == 3_000_000_000
