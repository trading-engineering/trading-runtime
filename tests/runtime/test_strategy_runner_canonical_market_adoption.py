from __future__ import annotations

from collections import deque
from types import SimpleNamespace
from typing import Any

import pytest
from tradingchassis_core.core.domain.configuration import CoreConfiguration
from tradingchassis_core.core.domain.processing_step import (
    ControlTimeQueueReevaluationContext,
    CoreExecutionControlApplyContext,
    CorePolicyAdmissionContext,
)
from tradingchassis_core.core.domain.state import StrategyState
from tradingchassis_core.core.domain.step_result import CoreStepResult
from tradingchassis_core.core.domain.types import (
    BookLevel,
    BookPayload,
    CancelOrderIntent,
    ControlTimeEvent,
    FillEvent,
    MarketEvent,
    NewOrderIntent,
    OrderSubmittedEvent,
    Price,
    Quantity,
    ReplaceOrderIntent,
)
from tradingchassis_core.core.events.event_bus import EventBus
from tradingchassis_core.core.execution_control.types import (
    ControlSchedulingObligation,
)
from tradingchassis_core.core.risk.risk_config import RiskConfig
from tradingchassis_core.core.risk.risk_engine import GateDecision
from tradingchassis_core.strategies.base import Strategy

import core_runtime.backtest.engine.strategy_runner as strategy_runner_module
from core_runtime.backtest.engine.event_stream_cursor import EventStreamCursor
from core_runtime.backtest.engine.hft_engine import HftEngineConfig
from core_runtime.backtest.engine.strategy_runner import (
    MAX_TIMEOUT_NS,
    HftStrategyRunner,
)


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
        self.wait_calls: list[tuple[int, bool]] = []

    def wait_next(self, *, timeout_ns: int, include_order_resp: bool) -> int:
        self.wait_calls.append((timeout_ns, include_order_resp))
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


def _risk_cfg_with_rate_limits(
    *,
    max_orders_per_second: float,
    max_cancels_per_second: float,
) -> RiskConfig:
    return RiskConfig(
        scope="test",
        notional_limits={"currency": "USDC", "max_gross_notional": 1.0},
        order_rate_limits={
            "max_orders_per_second": max_orders_per_second,
            "max_cancels_per_second": max_cancels_per_second,
        },
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


def _decision_for(
    accepted_now: list[Any],
    *,
    next_send_ts_ns_local: int | None = None,
    control_scheduling_obligations: tuple[ControlSchedulingObligation, ...] = (),
) -> GateDecision:
    return GateDecision(
        ts_ns_local=2,
        accepted_now=accepted_now,
        queued=[],
        rejected=[],
        replaced_in_queue=[],
        dropped_in_queue=[],
        handled_in_queue=[],
        execution_rejected=[],
        next_send_ts_ns_local=next_send_ts_ns_local,
        control_scheduling_obligations=control_scheduling_obligations,
    )


def _obligation(
    *,
    due_ts_ns_local: int,
    obligation_key: str,
    reason: str = "rate_limit",
) -> ControlSchedulingObligation:
    return ControlSchedulingObligation(
        due_ts_ns_local=due_ts_ns_local,
        reason=reason,
        scope_key="instrument:BTC_USDC-PERPETUAL",
        source="execution_control_rate_limit",
        obligation_key=obligation_key,
    )


def _runner_for_scheduling_helpers() -> HftStrategyRunner:
    runner = object.__new__(HftStrategyRunner)
    runner._pending_control_scheduling_obligation = None
    runner._next_send_ts_ns_local = None
    runner._last_injected_control_deadline_ns = None
    return runner


def test_select_effective_control_scheduling_obligation_collapses_deterministically() -> None:
    decision = _decision_for(
        [],
        control_scheduling_obligations=(
            _obligation(due_ts_ns_local=7, obligation_key="z-key"),
            _obligation(due_ts_ns_local=5, obligation_key="z-key"),
            _obligation(due_ts_ns_local=5, obligation_key="a-key"),
        ),
    )

    selected = HftStrategyRunner._select_effective_control_scheduling_obligation(decision)

    assert selected is not None
    assert selected.due_ts_ns_local == 5
    assert selected.obligation_key == "a-key"


def test_apply_control_scheduling_decision_sets_pending_and_mirror() -> None:
    runner = _runner_for_scheduling_helpers()
    obligation = _obligation(due_ts_ns_local=5, obligation_key="k1")
    decision = _decision_for([], control_scheduling_obligations=(obligation,))

    runner._apply_control_scheduling_decision(decision)

    assert runner._pending_control_scheduling_obligation == obligation
    assert runner._next_send_ts_ns_local == 5


def test_apply_control_scheduling_decision_replaces_pending_same_due_different_key() -> None:
    runner = _runner_for_scheduling_helpers()
    obligation_a = _obligation(due_ts_ns_local=5, obligation_key="a-key")
    obligation_b = _obligation(due_ts_ns_local=5, obligation_key="b-key")

    runner._apply_control_scheduling_decision(
        _decision_for([], control_scheduling_obligations=(obligation_a,))
    )
    runner._apply_control_scheduling_decision(
        _decision_for([], control_scheduling_obligations=(obligation_b,))
    )

    assert runner._pending_control_scheduling_obligation == obligation_b
    assert runner._next_send_ts_ns_local == 5


def test_apply_control_scheduling_decision_clears_pending_when_no_obligation() -> None:
    runner = _runner_for_scheduling_helpers()
    obligation = _obligation(due_ts_ns_local=5, obligation_key="k1")
    runner._apply_control_scheduling_decision(
        _decision_for([], control_scheduling_obligations=(obligation,))
    )

    runner._apply_control_scheduling_decision(_decision_for([]))

    assert runner._pending_control_scheduling_obligation is None
    assert runner._next_send_ts_ns_local is None


def test_apply_control_scheduling_decision_scalar_fallback_without_structured_obligation() -> None:
    runner = _runner_for_scheduling_helpers()
    decision = _decision_for([], next_send_ts_ns_local=12)

    runner._apply_control_scheduling_decision(decision)

    assert runner._pending_control_scheduling_obligation is None
    assert runner._next_send_ts_ns_local == 12


def test_wakeup_collapse_flag_defaults_to_false() -> None:
    runner = HftStrategyRunner(
        engine_cfg=_engine_cfg(),
        strategy=_NoopStrategy(),
        risk_cfg=_risk_cfg(),
        core_cfg=_core_cfg(),
    )
    assert runner._enable_core_step_wakeup_collapse is False


def test_wakeup_collapse_flag_requires_market_core_step_flag() -> None:
    with pytest.raises(
        ValueError,
        match=(
            "enable_core_step_wakeup_collapse=True requires "
            "enable_core_step_market_dispatch=True"
        ),
    ):
        HftStrategyRunner(
            engine_cfg=_engine_cfg(),
            strategy=_NoopStrategy(),
            risk_cfg=_risk_cfg(),
            core_cfg=_core_cfg(),
            enable_core_step_market_dispatch=False,
            enable_core_step_control_time_dispatch=True,
            enable_core_step_wakeup_collapse=True,
        )


def test_wakeup_collapse_flag_requires_control_core_step_flag() -> None:
    with pytest.raises(
        ValueError,
        match=(
            "enable_core_step_wakeup_collapse=True requires "
            "enable_core_step_control_time_dispatch=True"
        ),
    ):
        HftStrategyRunner(
            engine_cfg=_engine_cfg(),
            strategy=_NoopStrategy(),
            risk_cfg=_risk_cfg(),
            core_cfg=_core_cfg(),
            enable_core_step_market_dispatch=True,
            enable_core_step_control_time_dispatch=False,
            enable_core_step_wakeup_collapse=True,
        )


def test_wakeup_collapse_flag_accepts_when_both_core_step_flags_enabled() -> None:
    runner = HftStrategyRunner(
        engine_cfg=_engine_cfg(),
        strategy=_NoopStrategy(),
        risk_cfg=_risk_cfg(),
        core_cfg=_core_cfg(),
        enable_core_step_market_dispatch=True,
        enable_core_step_control_time_dispatch=True,
        enable_core_step_wakeup_collapse=True,
    )
    assert runner._enable_core_step_wakeup_collapse is True


def test_process_market_event_routes_through_event_entry_with_core_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = object.__new__(HftStrategyRunner)
    runner.strategy_state = object()
    runner.strategy = _NoopStrategy()
    runner.engine_cfg = _engine_cfg()
    runner._core_cfg = _core_cfg()
    runner._event_stream_cursor = EventStreamCursor()

    captured: list[tuple[int, object, object | None]] = []

    def _spy_run_core_step(
        state: object,
        entry: object,
        *,
        configuration: object | None = None,
        control_time_queue_context: object | None = None,
        policy_admission_context: object | None = None,
        execution_control_apply_context: object | None = None,
        core_decision_context: object | None = None,
        strategy_evaluator: object | None = None,
    ) -> CoreStepResult:
        _ = state
        _ = core_decision_context
        assert policy_admission_context is None
        assert execution_control_apply_context is None
        captured.append((entry.position.index, configuration, strategy_evaluator))
        assert control_time_queue_context is None
        return CoreStepResult()

    monkeypatch.setattr(
        strategy_runner_module,
        "run_core_step",
        _spy_run_core_step,
    )

    runner._process_canonical_market_event(_market_event(1), constraints=SimpleNamespace())
    runner._process_canonical_market_event(_market_event(2), constraints=SimpleNamespace())

    assert [idx for idx, _, _ in captured] == [0, 1]
    assert captured[0][1] is runner._core_cfg
    assert captured[1][1] is runner._core_cfg
    assert captured[0][2] is not None
    assert captured[1][2] is not None
    assert runner._event_stream_cursor.next_index == 2


def test_first_canonical_event_uses_processing_position_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = object.__new__(HftStrategyRunner)
    runner.strategy_state = object()
    runner.strategy = _NoopStrategy()
    runner.engine_cfg = _engine_cfg()
    runner._core_cfg = _core_cfg()
    runner._event_stream_cursor = EventStreamCursor()

    captured: list[int] = []

    def _spy_run_core_step(
        state: object,
        entry: object,
        *,
        configuration: object | None = None,
        control_time_queue_context: object | None = None,
        policy_admission_context: object | None = None,
        execution_control_apply_context: object | None = None,
        core_decision_context: object | None = None,
        strategy_evaluator: object | None = None,
    ) -> CoreStepResult:
        _ = state
        _ = (control_time_queue_context, core_decision_context, strategy_evaluator)
        assert policy_admission_context is None
        assert execution_control_apply_context is None
        assert configuration is runner._core_cfg
        captured.append(entry.position.index)
        return CoreStepResult()

    monkeypatch.setattr(
        strategy_runner_module,
        "run_core_step",
        _spy_run_core_step,
    )

    runner._process_canonical_market_event(_market_event(1), constraints=SimpleNamespace())

    assert captured == [0]
    assert runner._event_stream_cursor.next_index == 1


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

    captured: list[tuple[int, object, str]] = []

    def _spy_run_core_step(
        state: object,
        entry: object,
        *,
        configuration: object | None = None,
        control_time_queue_context: object | None = None,
        policy_admission_context: object | None = None,
        execution_control_apply_context: object | None = None,
        core_decision_context: object | None = None,
        strategy_evaluator: object | None = None,
    ) -> CoreStepResult:
        _ = state
        _ = (control_time_queue_context, core_decision_context)
        assert policy_admission_context is None
        assert execution_control_apply_context is None
        assert strategy_evaluator is not None
        captured.append((entry.position.index, configuration, type(entry.event).__name__))
        return CoreStepResult()

    monkeypatch.setattr(
        strategy_runner_module,
        "run_core_step",
        _spy_run_core_step,
    )

    venue = _StubVenue(
        rc_sequence=[0, 2, 1],
        ts_sequence=[1, 2, 3],
        depth=_depth_snapshot(),
    )
    runner.run(venue=venue, execution=_NoopExecution(), recorder=_RecorderWrapper())

    assert captured == [(0, runner._core_cfg, "MarketEvent")]


def test_wait_next_bootstrap_uses_include_order_resp_false_then_true_in_loop() -> None:
    runner = HftStrategyRunner(
        engine_cfg=_engine_cfg(),
        strategy=_NoopStrategy(),
        risk_cfg=_risk_cfg(),
        core_cfg=_core_cfg(),
    )

    venue = _StubVenue(
        rc_sequence=[0, 2, 1],
        ts_sequence=[1, 2, 3],
        depth=_depth_snapshot(),
    )
    runner.run(venue=venue, execution=_NoopExecution(), recorder=_RecorderWrapper())

    assert len(venue.wait_calls) >= 2
    first_timeout_ns, first_include_order_resp = venue.wait_calls[0]
    assert first_timeout_ns == MAX_TIMEOUT_NS
    assert first_include_order_resp is False
    assert all(include_order_resp is True for _, include_order_resp in venue.wait_calls[1:])


def test_market_mapping_from_depth_snapshot_is_deterministic_golden(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = HftStrategyRunner(
        engine_cfg=_engine_cfg(),
        strategy=_NoopStrategy(),
        risk_cfg=_risk_cfg(),
        core_cfg=_core_cfg(),
    )

    captured_market_events: list[MarketEvent] = []

    def _spy_run_core_step(
        state: object,
        entry: object,
        *,
        configuration: object | None = None,
        control_time_queue_context: object | None = None,
        policy_admission_context: object | None = None,
        execution_control_apply_context: object | None = None,
        core_decision_context: object | None = None,
        strategy_evaluator: object | None = None,
    ) -> CoreStepResult:
        _ = (state, configuration)
        _ = (control_time_queue_context, core_decision_context, strategy_evaluator)
        assert policy_admission_context is None
        assert execution_control_apply_context is None
        if isinstance(entry.event, MarketEvent):
            captured_market_events.append(entry.event)
        return CoreStepResult()

    monkeypatch.setattr(
        strategy_runner_module,
        "run_core_step",
        _spy_run_core_step,
    )

    venue = _StubVenue(
        rc_sequence=[0, 2, 1],
        ts_sequence=[1, 2_000_000_000, 2_000_000_001],
        depth=_depth_snapshot(),
    )
    runner.run(venue=venue, execution=_NoopExecution(), recorder=_RecorderWrapper())

    assert len(captured_market_events) == 1
    market_event = captured_market_events[0]
    assert market_event.instrument == "BTC_USDC-PERPETUAL"
    assert market_event.ts_ns_local == 2_000_000_000
    assert market_event.ts_ns_exch == 2_000_000_000
    assert market_event.book is not None
    assert market_event.book.bids[0].price.value == 10.0
    assert market_event.book.asks[0].price.value == 10.100000000000001
    assert market_event.book.bids[0].quantity.value == 1.0
    assert market_event.book.asks[0].quantity.value == 0.0


def test_market_branch_strategy_evaluator_preserves_legacy_on_feed_arguments(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    generated_intent = _new_intent(ts_ns_local=2_000_000_000)
    constraints_obj = SimpleNamespace(tag="constraints")
    on_feed_calls: list[tuple[object, object, object, object]] = []
    risk_calls: list[list[object]] = []

    class _SpyStrategy(Strategy):
        def on_feed(self, state: Any, event: Any, engine_cfg: Any, constraints: Any) -> list[Any]:
            on_feed_calls.append((state, event, engine_cfg, constraints))
            return [generated_intent]

        def on_order_update(self, state: Any, engine_cfg: Any, constraints: Any) -> list[Any]:
            _ = (state, engine_cfg, constraints)
            return []

        def on_risk_decision(self, decision: Any) -> None:
            _ = decision

    runner = HftStrategyRunner(
        engine_cfg=_engine_cfg(),
        strategy=_SpyStrategy(),
        risk_cfg=_risk_cfg(),
        core_cfg=_core_cfg(),
    )
    monkeypatch.setattr(runner.risk, "build_constraints", lambda _ts: constraints_obj)

    def _spy_run_core_step(
        state: object,
        entry: object,
        *,
        configuration: object | None = None,
        control_time_queue_context: object | None = None,
        policy_admission_context: object | None = None,
        execution_control_apply_context: object | None = None,
        core_decision_context: object | None = None,
        strategy_evaluator: object | None = None,
    ) -> CoreStepResult:
        _ = core_decision_context
        assert control_time_queue_context is None
        assert policy_admission_context is None
        assert execution_control_apply_context is None
        assert strategy_evaluator is not None
        evaluated = strategy_evaluator.evaluate(
            SimpleNamespace(
                state=state,
                event=entry.event,
                position=entry.position,
                configuration=configuration,
            )
        )
        return CoreStepResult(generated_intents=tuple(evaluated))

    def _spy_decide_intents(**kwargs: object) -> GateDecision:
        risk_calls.append(list(kwargs["raw_intents"]))
        return _decision_for([])

    monkeypatch.setattr(strategy_runner_module, "run_core_step", _spy_run_core_step)
    monkeypatch.setattr(runner.risk, "decide_intents", _spy_decide_intents)

    venue = _StubVenue(
        rc_sequence=[0, 2, 1],
        ts_sequence=[1, 2_000_000_000, 2_000_000_001],
        depth=_depth_snapshot(),
    )
    runner.run(venue=venue, execution=_NoopExecution(), recorder=_RecorderWrapper())

    assert len(on_feed_calls) == 1
    state_arg, event_arg, engine_cfg_arg, constraints_arg = on_feed_calls[0]
    assert state_arg is runner.strategy_state
    assert isinstance(event_arg, MarketEvent)
    assert engine_cfg_arg is runner.engine_cfg
    assert constraints_arg is constraints_obj
    assert len(risk_calls) == 1
    assert risk_calls[0] == [generated_intent]


def test_market_core_step_mode_calls_run_core_step_with_policy_and_apply_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = HftStrategyRunner(
        engine_cfg=_engine_cfg(),
        strategy=_NoopStrategy(),
        risk_cfg=_risk_cfg_with_rate_limits(
            max_orders_per_second=7.0,
            max_cancels_per_second=3.0,
        ),
        core_cfg=_core_cfg(),
        enable_core_step_market_dispatch=True,
    )

    captured: list[tuple[object, object, object, object]] = []

    def _spy_run_core_step(
        state: object,
        entry: object,
        *,
        configuration: object | None = None,
        control_time_queue_context: object | None = None,
        policy_admission_context: object | None = None,
        execution_control_apply_context: object | None = None,
        core_decision_context: object | None = None,
        strategy_evaluator: object | None = None,
    ) -> CoreStepResult:
        _ = core_decision_context
        assert control_time_queue_context is None
        assert strategy_evaluator is not None
        captured.append(
            (
                state,
                configuration,
                policy_admission_context,
                execution_control_apply_context,
            )
        )
        return CoreStepResult(generated_intents=(_new_intent(),))

    monkeypatch.setattr(strategy_runner_module, "run_core_step", _spy_run_core_step)
    monkeypatch.setattr(
        runner.risk,
        "decide_intents",
        lambda **_: (_ for _ in ()).throw(
            AssertionError("market core-step mode must not call runtime risk gate")
        ),
    )

    venue = _StubVenue(
        rc_sequence=[0, 2, 1],
        ts_sequence=[1, 2_000_000_000, 2_000_000_001],
        depth=_depth_snapshot(),
    )
    runner.run(venue=venue, execution=_NoopExecution(), recorder=_RecorderWrapper())

    assert len(captured) == 1
    state, configuration, policy_ctx, apply_ctx = captured[0]
    assert state is runner.strategy_state
    assert configuration is runner._core_cfg
    assert isinstance(policy_ctx, CorePolicyAdmissionContext)
    assert policy_ctx.policy_evaluator is runner.risk
    assert policy_ctx.now_ts_ns_local == 2_000_000_000
    assert isinstance(apply_ctx, CoreExecutionControlApplyContext)
    assert apply_ctx.execution_control is runner.risk.execution_control
    assert apply_ctx.now_ts_ns_local == 2_000_000_000
    assert apply_ctx.max_orders_per_sec == 7.0
    assert apply_ctx.max_cancels_per_sec == 3.0
    assert apply_ctx.activate_dispatchable_outputs is True


def test_market_core_step_mode_dispatches_core_step_dispatchable_intents_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    generated_intent = _new_intent(ts_ns_local=2)
    dispatchable_intent = _new_intent(ts_ns_local=2).model_copy(
        update={"client_order_id": "cid-dispatchable-only"}
    )
    runner = HftStrategyRunner(
        engine_cfg=_engine_cfg(),
        strategy=_EmitIntentsStrategy([generated_intent]),
        risk_cfg=_risk_cfg(),
        core_cfg=_core_cfg(),
        enable_core_step_market_dispatch=True,
    )

    class _ExecutionCapture:
        def __init__(self) -> None:
            self.batches: list[list[str]] = []

        def apply_intents(self, intents: list[Any]) -> list[tuple[Any, str]]:
            self.batches.append([it.client_order_id for it in intents])
            return []

    execution = _ExecutionCapture()

    def _spy_run_core_step(
        state: object,
        entry: object,
        *,
        configuration: object | None = None,
        control_time_queue_context: object | None = None,
        policy_admission_context: object | None = None,
        execution_control_apply_context: object | None = None,
        core_decision_context: object | None = None,
        strategy_evaluator: object | None = None,
    ) -> CoreStepResult:
        _ = (
            state,
            entry,
            configuration,
            control_time_queue_context,
            policy_admission_context,
            execution_control_apply_context,
            core_decision_context,
            strategy_evaluator,
        )
        return CoreStepResult(
            generated_intents=(generated_intent,),
            dispatchable_intents=(dispatchable_intent,),
        )

    monkeypatch.setattr(strategy_runner_module, "run_core_step", _spy_run_core_step)
    monkeypatch.setattr(
        runner.risk,
        "decide_intents",
        lambda **_: (_ for _ in ()).throw(
            AssertionError("market core-step mode must not call runtime risk gate")
        ),
    )

    venue = _StubVenue(
        rc_sequence=[0, 2, 1],
        ts_sequence=[1, 2, 3],
        depth=_depth_snapshot(),
    )
    runner.run(venue=venue, execution=execution, recorder=_RecorderWrapper())

    assert execution.batches == [[dispatchable_intent.client_order_id]]


def test_market_core_step_mode_applies_obligation_and_clears_when_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = HftStrategyRunner(
        engine_cfg=_engine_cfg(),
        strategy=_NoopStrategy(),
        risk_cfg=_risk_cfg(),
        core_cfg=_core_cfg(),
        enable_core_step_market_dispatch=True,
    )
    seeded = _obligation(due_ts_ns_local=9_999_999_999, obligation_key="seeded")
    runner._pending_control_scheduling_obligation = seeded
    runner._next_send_ts_ns_local = seeded.due_ts_ns_local
    obligation = _obligation(due_ts_ns_local=25, obligation_key="core-step-obligation")

    results = [
        CoreStepResult(control_scheduling_obligation=obligation),
        CoreStepResult(control_scheduling_obligation=None),
    ]

    def _spy_run_core_step(
        state: object,
        entry: object,
        *,
        configuration: object | None = None,
        control_time_queue_context: object | None = None,
        policy_admission_context: object | None = None,
        execution_control_apply_context: object | None = None,
        core_decision_context: object | None = None,
        strategy_evaluator: object | None = None,
    ) -> CoreStepResult:
        _ = (
            state,
            entry,
            configuration,
            control_time_queue_context,
            policy_admission_context,
            execution_control_apply_context,
            core_decision_context,
            strategy_evaluator,
        )
        return results.pop(0)

    monkeypatch.setattr(strategy_runner_module, "run_core_step", _spy_run_core_step)
    monkeypatch.setattr(
        runner.risk,
        "decide_intents",
        lambda **_: (_ for _ in ()).throw(
            AssertionError("market core-step mode must not call runtime risk gate")
        ),
    )

    venue = _StubVenue(
        rc_sequence=[0, 2, 2, 1],
        ts_sequence=[1, 2, 3, 4],
        depth=_depth_snapshot(),
    )
    runner.run(venue=venue, execution=_NoopExecution(), recorder=_RecorderWrapper())

    assert runner._pending_control_scheduling_obligation is None
    assert runner._next_send_ts_ns_local is None


def test_market_core_step_mode_preserves_order_submitted_before_mark_sent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dispatchable_new = _new_intent()
    runner = HftStrategyRunner(
        engine_cfg=_engine_cfg(),
        strategy=_NoopStrategy(),
        risk_cfg=_risk_cfg(),
        core_cfg=_core_cfg(),
        enable_core_step_market_dispatch=True,
    )
    ordering: list[str] = []
    submitted_events: list[OrderSubmittedEvent] = []
    marks: list[tuple[str, str, str]] = []

    monkeypatch.setattr(
        strategy_runner_module,
        "run_core_step",
        lambda *args, **kwargs: CoreStepResult(
            dispatchable_intents=(dispatchable_new,),
        ),
    )
    monkeypatch.setattr(
        runner.risk,
        "decide_intents",
        lambda **_: (_ for _ in ()).throw(
            AssertionError("market core-step mode must not call runtime risk gate")
        ),
    )

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
    monkeypatch.setattr(
        runner.strategy,
        "on_risk_decision",
        lambda _decision: (_ for _ in ()).throw(
            AssertionError("market core-step mode must not synthesize GateDecision callbacks")
        ),
    )

    venue = _StubVenue(
        rc_sequence=[0, 2, 1],
        ts_sequence=[1_111, 5_000_000_000, 5_000_000_001],
        depth=_depth_snapshot(),
    )
    runner.run(venue=venue, execution=_NoopExecution(), recorder=_RecorderWrapper())

    assert len(submitted_events) == 1
    assert ordering == ["submitted", "mark"]
    assert marks == [
        (dispatchable_new.instrument, dispatchable_new.client_order_id, "new")
    ]
    assert runner._last_core_step_execution_errors == []


def test_market_core_step_mode_failed_new_dispatch_emits_no_order_submitted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dispatchable_new = _new_intent()
    runner = HftStrategyRunner(
        engine_cfg=_engine_cfg(),
        strategy=_NoopStrategy(),
        risk_cfg=_risk_cfg(),
        core_cfg=_core_cfg(),
        enable_core_step_market_dispatch=True,
    )
    submitted_event_count = 0
    marked_count = 0

    monkeypatch.setattr(
        strategy_runner_module,
        "run_core_step",
        lambda *args, **kwargs: CoreStepResult(
            dispatchable_intents=(dispatchable_new,),
        ),
    )
    monkeypatch.setattr(
        runner.risk,
        "decide_intents",
        lambda **_: (_ for _ in ()).throw(
            AssertionError("market core-step mode must not call runtime risk gate")
        ),
    )

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
        lambda _decision: (_ for _ in ()).throw(
            AssertionError("market core-step mode must not synthesize GateDecision callbacks")
        ),
    )

    class _ExecutionFailNew:
        def apply_intents(self, intents: list[Any]) -> list[tuple[Any, str]]:
            _ = intents
            return [(dispatchable_new, "EXCHANGE_REJECT")]

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
    assert len(runner._last_core_step_execution_errors) == 1
    failed_intent, failure_reason = runner._last_core_step_execution_errors[0]
    assert failed_intent.client_order_id == dispatchable_new.client_order_id
    assert failure_reason == "EXCHANGE_REJECT"


def test_market_core_step_mode_replace_cancel_emit_no_order_submitted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    replace_intent = _replace_intent()
    cancel_intent = _cancel_intent()
    runner = HftStrategyRunner(
        engine_cfg=_engine_cfg(),
        strategy=_NoopStrategy(),
        risk_cfg=_risk_cfg(),
        core_cfg=_core_cfg(),
        enable_core_step_market_dispatch=True,
    )
    submitted_event_count = 0
    marks: list[tuple[str, str, str]] = []

    monkeypatch.setattr(
        strategy_runner_module,
        "run_core_step",
        lambda *args, **kwargs: CoreStepResult(
            dispatchable_intents=(replace_intent, cancel_intent),
        ),
    )
    monkeypatch.setattr(
        runner.risk,
        "decide_intents",
        lambda **_: (_ for _ in ()).throw(
            AssertionError("market core-step mode must not call runtime risk gate")
        ),
    )

    def _spy_process_event_entry(state: object, entry: object, *, configuration: object) -> None:
        nonlocal submitted_event_count
        _ = (state, configuration)
        if isinstance(entry.event, OrderSubmittedEvent):
            submitted_event_count += 1

    monkeypatch.setattr(strategy_runner_module, "process_event_entry", _spy_process_event_entry)
    monkeypatch.setattr(
        runner.strategy_state,
        "mark_intent_sent",
        lambda i, c, t: marks.append((i, c, t)),
    )

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
    assert runner._last_core_step_execution_errors == []


def test_market_core_step_mode_failure_does_not_commit_or_dispatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = HftStrategyRunner(
        engine_cfg=_engine_cfg(),
        strategy=_NoopStrategy(),
        risk_cfg=_risk_cfg(),
        core_cfg=_core_cfg(),
        enable_core_step_market_dispatch=True,
    )

    monkeypatch.setattr(
        strategy_runner_module,
        "run_core_step",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            RuntimeError("boom-market-core-step")
        ),
    )
    monkeypatch.setattr(
        runner.risk,
        "decide_intents",
        lambda **_: (_ for _ in ()).throw(
            AssertionError("risk gate must not run after market core-step failure")
        ),
    )

    class _ExecutionMustNotRun:
        def apply_intents(self, intents: list[Any]) -> list[tuple[Any, str]]:
            _ = intents
            raise AssertionError("dispatch must not run after market core-step failure")

    venue = _StubVenue(
        rc_sequence=[0, 2, 1],
        ts_sequence=[1, 2, 3],
        depth=_depth_snapshot(),
    )
    with pytest.raises(RuntimeError, match="boom-market-core-step"):
        runner.run(venue=venue, execution=_ExecutionMustNotRun(), recorder=_RecorderWrapper())

    assert runner._event_stream_cursor.next_index == 0


def test_order_update_path_remains_legacy_when_market_core_step_mode_enabled() -> None:
    order_update_intent = _new_intent(ts_ns_local=2)
    risk_calls: list[list[object]] = []

    class _OrderUpdateStrategy(Strategy):
        def on_feed(self, state: Any, event: Any, engine_cfg: Any, constraints: Any) -> list[Any]:
            _ = (state, event, engine_cfg, constraints)
            return []

        def on_order_update(self, state: Any, engine_cfg: Any, constraints: Any) -> list[Any]:
            _ = (state, engine_cfg, constraints)
            return [order_update_intent]

        def on_risk_decision(self, decision: Any) -> None:
            _ = decision

    runner = HftStrategyRunner(
        engine_cfg=_engine_cfg(),
        strategy=_OrderUpdateStrategy(),
        risk_cfg=_risk_cfg(),
        core_cfg=_core_cfg(),
        enable_core_step_market_dispatch=True,
    )

    def _spy_decide_intents(**kwargs: object) -> GateDecision:
        risk_calls.append(list(kwargs["raw_intents"]))
        return _decision_for([])

    runner.risk.decide_intents = _spy_decide_intents  # type: ignore[method-assign]

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

    assert len(risk_calls) == 1
    assert risk_calls[0] == [order_update_intent]


def test_market_run_core_step_failure_does_not_commit_or_reach_risk_dispatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = HftStrategyRunner(
        engine_cfg=_engine_cfg(),
        strategy=_NoopStrategy(),
        risk_cfg=_risk_cfg(),
        core_cfg=_core_cfg(),
    )

    def _fail_run_core_step(*args: object, **kwargs: object) -> CoreStepResult:
        _ = (args, kwargs)
        raise RuntimeError("boom-market-core-step")

    monkeypatch.setattr(strategy_runner_module, "run_core_step", _fail_run_core_step)
    monkeypatch.setattr(
        runner.risk,
        "decide_intents",
        lambda **_: (_ for _ in ()).throw(
            AssertionError("risk gate must not run after market core-step failure")
        ),
    )

    venue = _StubVenue(
        rc_sequence=[0, 2, 1],
        ts_sequence=[1, 2, 3],
        depth=_depth_snapshot(),
    )
    with pytest.raises(RuntimeError, match="boom-market-core-step"):
        runner.run(venue=venue, execution=_NoopExecution(), recorder=_RecorderWrapper())

    assert runner._event_stream_cursor.next_index == 0


def test_missing_core_cfg_fails_before_market_mutation() -> None:
    runner = object.__new__(HftStrategyRunner)
    runner.strategy_state = StrategyState(event_bus=EventBus(sinks=[]))
    runner.strategy = _NoopStrategy()
    runner.engine_cfg = _engine_cfg()
    runner._core_cfg = None
    runner._event_stream_cursor = EventStreamCursor()

    with pytest.raises(ValueError, match="CoreConfiguration is required"):
        runner._process_canonical_market_event(
            _market_event(42),
            constraints=SimpleNamespace(),
        )

    assert runner.strategy_state.market == {}
    assert runner.strategy_state._last_processing_position_index is None
    assert runner._event_stream_cursor.next_index == 0


def test_invalid_core_cfg_type_fails_before_market_mutation() -> None:
    runner = object.__new__(HftStrategyRunner)
    runner.strategy_state = StrategyState(event_bus=EventBus(sinks=[]))
    runner.strategy = _NoopStrategy()
    runner.engine_cfg = _engine_cfg()
    runner._core_cfg = object()
    runner._event_stream_cursor = EventStreamCursor()

    with pytest.raises(TypeError, match="configuration must be CoreConfiguration or None"):
        runner._process_canonical_market_event(
            _market_event(42),
            constraints=SimpleNamespace(),
        )

    assert runner.strategy_state.market == {}
    assert runner.strategy_state._last_processing_position_index is None
    assert runner._event_stream_cursor.next_index == 0


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
    assert runner._event_stream_cursor.next_index == 0


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
    assert runner._event_stream_cursor.next_index == 0


def test_rc2_rc3_paths_never_emit_fill_event_through_process_event_entry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = HftStrategyRunner(
        engine_cfg=_engine_cfg(),
        strategy=_NoopStrategy(),
        risk_cfg=_risk_cfg(),
        core_cfg=_core_cfg(),
    )

    calls = {"update_account": 0, "ingest_order_snapshots": 0}
    emitted_fill_events = 0

    def _spy_update_account(*args: object, **kwargs: object) -> None:
        _ = (args, kwargs)
        calls["update_account"] += 1

    def _spy_ingest_order_snapshots(*args: object, **kwargs: object) -> None:
        _ = (args, kwargs)
        calls["ingest_order_snapshots"] += 1

    def _spy_process_event_entry(state: object, entry: object, *, configuration: object) -> None:
        nonlocal emitted_fill_events
        _ = (state, configuration)
        if isinstance(entry.event, FillEvent):
            emitted_fill_events += 1

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
        rc_sequence=[0, 2, 3, 1],
        ts_sequence=[1, 2, 3, 4],
        depth=_depth_snapshot(),
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

    assert emitted_fill_events == 0
    assert calls["update_account"] == 1
    assert calls["ingest_order_snapshots"] == 1


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

    def _spy_run_core_step(
        state: object,
        entry: object,
        *,
        configuration: object | None = None,
        control_time_queue_context: object | None = None,
        policy_admission_context: object | None = None,
        execution_control_apply_context: object | None = None,
        core_decision_context: object | None = None,
        strategy_evaluator: object | None = None,
    ) -> CoreStepResult:
        _ = (state, configuration, core_decision_context, strategy_evaluator)
        assert control_time_queue_context is None
        assert policy_admission_context is None
        assert execution_control_apply_context is None
        positions.append((entry.position.index, type(entry.event).__name__))
        return CoreStepResult(generated_intents=(new_intent,))

    monkeypatch.setattr(strategy_runner_module, "process_event_entry", _spy_process_event_entry)
    monkeypatch.setattr(strategy_runner_module, "run_core_step", _spy_run_core_step)

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
    assert runner._event_stream_cursor.next_index == 2


def test_canonical_counter_increments_only_after_successful_canonical_processing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = object.__new__(HftStrategyRunner)
    runner.strategy_state = object()
    runner.strategy = _NoopStrategy()
    runner.engine_cfg = _engine_cfg()
    runner._core_cfg = _core_cfg()
    runner._event_stream_cursor = EventStreamCursor()

    def _fail(*args: object, **kwargs: object) -> CoreStepResult:
        _ = (args, kwargs)
        raise RuntimeError("boom")

    monkeypatch.setattr(strategy_runner_module, "run_core_step", _fail)
    with pytest.raises(RuntimeError, match="boom"):
        runner._process_canonical_market_event(
            _market_event(1),
            constraints=SimpleNamespace(),
        )
    assert runner._event_stream_cursor.next_index == 0

    called = {"count": 0}

    def _ok(*args: object, **kwargs: object) -> CoreStepResult:
        _ = (args, kwargs)
        called["count"] += 1
        return CoreStepResult()

    monkeypatch.setattr(strategy_runner_module, "run_core_step", _ok)
    runner._process_canonical_market_event(
        _market_event(2),
        constraints=SimpleNamespace(),
    )
    assert called["count"] == 1
    assert runner._event_stream_cursor.next_index == 1


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

    def _spy_run_core_step(
        state: object,
        entry: object,
        *,
        configuration: object | None = None,
        control_time_queue_context: object | None = None,
        policy_admission_context: object | None = None,
        execution_control_apply_context: object | None = None,
        core_decision_context: object | None = None,
        strategy_evaluator: object | None = None,
    ) -> CoreStepResult:
        _ = (state, configuration)
        _ = core_decision_context
        assert policy_admission_context is None
        assert execution_control_apply_context is None
        if isinstance(entry.event, MarketEvent):
            assert control_time_queue_context is None
            assert strategy_evaluator is not None
            return CoreStepResult(generated_intents=(_new_intent(),))
        assert isinstance(control_time_queue_context, ControlTimeQueueReevaluationContext)
        if isinstance(entry.event, ControlTimeEvent):
            control_events.append(entry.event)
        return CoreStepResult()

    monkeypatch.setattr(strategy_runner_module, "run_core_step", _spy_run_core_step)
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


def test_control_time_realization_routes_through_run_core_step_with_expected_arguments(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = HftStrategyRunner(
        engine_cfg=_engine_cfg(),
        strategy=_NoopStrategy(),
        risk_cfg=_risk_cfg(),
        core_cfg=_core_cfg(),
    )
    runner._next_send_ts_ns_local = 5

    captured_calls: list[tuple[object, object, object, object | None]] = []

    def _spy_run_core_step(
        state: object,
        entry: object,
        *,
        configuration: object | None = None,
        control_time_queue_context: object | None = None,
        policy_admission_context: object | None = None,
        execution_control_apply_context: object | None = None,
    ) -> CoreStepResult:
        captured_calls.append((state, entry, configuration, control_time_queue_context))
        assert policy_admission_context is None
        assert execution_control_apply_context is None
        return CoreStepResult()

    monkeypatch.setattr(strategy_runner_module, "run_core_step", _spy_run_core_step)
    venue = _StubVenue(
        rc_sequence=[0, 0, 1],
        ts_sequence=[1, 10, 11],
    )
    runner.run(venue=venue, execution=_NoopExecution(), recorder=_RecorderWrapper())

    assert len(captured_calls) == 1
    state, entry, configuration, control_time_queue_context = captured_calls[0]
    assert state is runner.strategy_state
    assert configuration is runner._core_cfg
    assert isinstance(entry.event, ControlTimeEvent)
    assert entry.position.index == 0
    assert isinstance(control_time_queue_context, ControlTimeQueueReevaluationContext)
    assert control_time_queue_context.risk_engine is runner.risk
    assert control_time_queue_context.instrument == runner.engine_cfg.instrument
    assert control_time_queue_context.now_ts_ns_local == 10
    assert runner._event_stream_cursor.next_index == 1


def test_control_time_event_uses_structured_obligation_fields_when_available(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = HftStrategyRunner(
        engine_cfg=_engine_cfg(),
        strategy=_EmitIntentsStrategy([_new_intent()]),
        risk_cfg=_risk_cfg(),
        core_cfg=_core_cfg(),
    )
    obligation = ControlSchedulingObligation(
        due_ts_ns_local=5,
        reason="custom_backpressure_reason",
        scope_key="instrument:BTC_USDC-PERPETUAL",
        source="execution_control_rate_limit",
    )
    monkeypatch.setattr(
        runner.risk,
        "decide_intents",
        lambda **_: _decision_for(
            [],
            next_send_ts_ns_local=5,
            control_scheduling_obligations=(obligation,),
        ),
    )

    control_events: list[ControlTimeEvent] = []

    def _spy_run_core_step(
        state: object,
        entry: object,
        *,
        configuration: object | None = None,
        control_time_queue_context: object | None = None,
        policy_admission_context: object | None = None,
        execution_control_apply_context: object | None = None,
        core_decision_context: object | None = None,
        strategy_evaluator: object | None = None,
    ) -> CoreStepResult:
        _ = (state, configuration)
        _ = core_decision_context
        assert policy_admission_context is None
        assert execution_control_apply_context is None
        if isinstance(entry.event, MarketEvent):
            assert control_time_queue_context is None
            assert strategy_evaluator is not None
            return CoreStepResult(generated_intents=(_new_intent(),))
        assert isinstance(control_time_queue_context, ControlTimeQueueReevaluationContext)
        if isinstance(entry.event, ControlTimeEvent):
            control_events.append(entry.event)
        return CoreStepResult()

    monkeypatch.setattr(strategy_runner_module, "run_core_step", _spy_run_core_step)
    venue = _StubVenue(
        rc_sequence=[0, 2, 0, 1],
        ts_sequence=[1, 2, 10, 11],
        depth=_depth_snapshot(),
    )
    runner.run(venue=venue, execution=_NoopExecution(), recorder=_RecorderWrapper())

    assert len(control_events) == 1
    event = control_events[0]
    assert event.reason == "scheduled_control_recheck"
    assert event.due_ts_ns_local == 5
    assert event.realized_ts_ns_local == 10
    assert event.obligation_reason == "custom_backpressure_reason"
    assert event.obligation_due_ts_ns_local == 5


def test_control_time_event_falls_back_when_structured_obligation_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = HftStrategyRunner(
        engine_cfg=_engine_cfg(),
        strategy=_NoopStrategy(),
        risk_cfg=_risk_cfg(),
        core_cfg=_core_cfg(),
    )
    runner._next_send_ts_ns_local = 5
    runner._pending_control_scheduling_obligation = None

    control_events: list[ControlTimeEvent] = []

    def _spy_run_core_step(
        state: object,
        entry: object,
        *,
        configuration: object | None = None,
        control_time_queue_context: object | None = None,
        policy_admission_context: object | None = None,
        execution_control_apply_context: object | None = None,
    ) -> CoreStepResult:
        _ = (state, configuration)
        assert policy_admission_context is None
        assert execution_control_apply_context is None
        assert isinstance(control_time_queue_context, ControlTimeQueueReevaluationContext)
        if isinstance(entry.event, ControlTimeEvent):
            control_events.append(entry.event)
        return CoreStepResult()

    monkeypatch.setattr(strategy_runner_module, "run_core_step", _spy_run_core_step)
    venue = _StubVenue(
        rc_sequence=[0, 0, 1],
        ts_sequence=[1, 10, 11],
    )
    runner.run(venue=venue, execution=_NoopExecution(), recorder=_RecorderWrapper())

    assert len(control_events) == 1
    event = control_events[0]
    assert event.obligation_reason == "rate_limit"
    assert event.obligation_due_ts_ns_local == 5


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

    def _spy_run_core_step(
        state: object,
        entry: object,
        *,
        configuration: object | None = None,
        control_time_queue_context: object | None = None,
        policy_admission_context: object | None = None,
        execution_control_apply_context: object | None = None,
    ) -> CoreStepResult:
        nonlocal control_count
        _ = (state, configuration)
        assert policy_admission_context is None
        assert execution_control_apply_context is None
        assert isinstance(control_time_queue_context, ControlTimeQueueReevaluationContext)
        if isinstance(entry.event, ControlTimeEvent):
            control_count += 1
        return CoreStepResult()

    monkeypatch.setattr(strategy_runner_module, "run_core_step", _spy_run_core_step)
    venue = _StubVenue(
        rc_sequence=[0, 0, 0, 1],
        ts_sequence=[1, 10, 10, 11],
    )
    runner.run(venue=venue, execution=_NoopExecution(), recorder=_RecorderWrapper())

    assert control_count == 1


def test_control_time_event_processed_through_core_step_context_without_runtime_pop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = HftStrategyRunner(
        engine_cfg=_engine_cfg(),
        strategy=_NoopStrategy(),
        risk_cfg=_risk_cfg(),
        core_cfg=_core_cfg(),
    )
    runner._next_send_ts_ns_local = 5

    def _spy_run_core_step(
        state: object,
        entry: object,
        *,
        configuration: object | None = None,
        control_time_queue_context: object | None = None,
        policy_admission_context: object | None = None,
        execution_control_apply_context: object | None = None,
    ) -> CoreStepResult:
        _ = (state, configuration)
        assert policy_admission_context is None
        assert execution_control_apply_context is None
        assert isinstance(control_time_queue_context, ControlTimeQueueReevaluationContext)
        if isinstance(entry.event, ControlTimeEvent):
            assert entry.position.index == 0
        return CoreStepResult()

    monkeypatch.setattr(
        runner.strategy_state,
        "pop_queued_intents",
        lambda _: (_ for _ in ()).throw(
            AssertionError("runtime must not pop queued intents for control-time re-evaluation")
        ),
    )
    monkeypatch.setattr(strategy_runner_module, "run_core_step", _spy_run_core_step)
    monkeypatch.setattr(
        runner.risk,
        "decide_intents",
        lambda **_: (_ for _ in ()).throw(
            AssertionError("runtime must not run risk gate for control-time queue directly")
        ),
    )

    venue = _StubVenue(
        rc_sequence=[0, 0, 1],
        ts_sequence=[1, 10, 11],
    )
    runner.run(venue=venue, execution=_NoopExecution(), recorder=_RecorderWrapper())

    assert runner._event_stream_cursor.next_index == 1


def test_control_time_processing_failure_does_not_mark_deadline_or_dispatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = HftStrategyRunner(
        engine_cfg=_engine_cfg(),
        strategy=_NoopStrategy(),
        risk_cfg=_risk_cfg(),
        core_cfg=_core_cfg(),
    )
    obligation = _obligation(due_ts_ns_local=5, obligation_key="k-failure")
    runner._pending_control_scheduling_obligation = obligation
    runner._next_send_ts_ns_local = obligation.due_ts_ns_local

    def _fail_run_core_step(
        state: object,
        entry: object,
        *,
        configuration: object | None = None,
        control_time_queue_context: object | None = None,
        policy_admission_context: object | None = None,
        execution_control_apply_context: object | None = None,
    ) -> CoreStepResult:
        _ = (state, configuration)
        assert policy_admission_context is None
        assert execution_control_apply_context is None
        assert isinstance(control_time_queue_context, ControlTimeQueueReevaluationContext)
        if isinstance(entry.event, ControlTimeEvent):
            raise RuntimeError("boom")
        return CoreStepResult()

    monkeypatch.setattr(strategy_runner_module, "run_core_step", _fail_run_core_step)
    monkeypatch.setattr(
        runner.strategy_state,
        "pop_queued_intents",
        lambda _: (_ for _ in ()).throw(
            AssertionError("runtime queue pop must not run on control-time failure")
        ),
    )

    venue = _StubVenue(
        rc_sequence=[0, 0, 1],
        ts_sequence=[1, 10, 11],
    )

    with pytest.raises(RuntimeError, match="boom"):
        runner.run(venue=venue, execution=_NoopExecution(), recorder=_RecorderWrapper())

    assert runner._last_injected_control_deadline_ns is None
    assert runner._pending_control_scheduling_obligation == obligation
    assert runner._next_send_ts_ns_local == 5
    assert runner._event_stream_cursor.next_index == 0


def test_market_and_order_submitted_paths_remain_on_process_event_entry_path(
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

    process_event_names: list[str] = []
    run_core_step_names: list[str] = []

    def _spy_process_event_entry(state: object, entry: object, *, configuration: object) -> None:
        _ = (state, configuration)
        process_event_names.append(type(entry.event).__name__)

    def _spy_run_core_step(
        state: object,
        entry: object,
        *,
        configuration: object | None = None,
        control_time_queue_context: object | None = None,
        policy_admission_context: object | None = None,
        execution_control_apply_context: object | None = None,
        core_decision_context: object | None = None,
        strategy_evaluator: object | None = None,
    ) -> CoreStepResult:
        _ = (state, configuration, core_decision_context, strategy_evaluator)
        assert control_time_queue_context is None
        assert policy_admission_context is None
        assert execution_control_apply_context is None
        run_core_step_names.append(type(entry.event).__name__)
        return CoreStepResult(generated_intents=(new_intent,))

    monkeypatch.setattr(strategy_runner_module, "process_event_entry", _spy_process_event_entry)
    monkeypatch.setattr(strategy_runner_module, "run_core_step", _spy_run_core_step)

    venue = _StubVenue(
        rc_sequence=[0, 2, 1],
        ts_sequence=[1, 10, 11],
        depth=_depth_snapshot(),
    )
    runner.run(venue=venue, execution=_NoopExecution(), recorder=_RecorderWrapper())

    assert run_core_step_names == ["MarketEvent"]
    assert process_event_names == ["OrderSubmittedEvent"]


def test_control_time_success_consumes_pending_obligation_after_core_step(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = HftStrategyRunner(
        engine_cfg=_engine_cfg(),
        strategy=_NoopStrategy(),
        risk_cfg=_risk_cfg(),
        core_cfg=_core_cfg(),
    )
    obligation = _obligation(due_ts_ns_local=5, obligation_key="k-success")
    runner._pending_control_scheduling_obligation = obligation
    runner._next_send_ts_ns_local = obligation.due_ts_ns_local

    def _spy_run_core_step(
        state: object,
        entry: object,
        *,
        configuration: object | None = None,
        control_time_queue_context: object | None = None,
        policy_admission_context: object | None = None,
        execution_control_apply_context: object | None = None,
    ) -> CoreStepResult:
        _ = (state, configuration)
        assert policy_admission_context is None
        assert execution_control_apply_context is None
        assert isinstance(control_time_queue_context, ControlTimeQueueReevaluationContext)
        return CoreStepResult()

    monkeypatch.setattr(strategy_runner_module, "run_core_step", _spy_run_core_step)
    monkeypatch.setattr(
        runner.strategy_state,
        "pop_queued_intents",
        lambda _: (_ for _ in ()).throw(
            AssertionError("runtime must not directly pop queued intents")
        ),
    )

    venue = _StubVenue(
        rc_sequence=[0, 0, 1],
        ts_sequence=[1, 10, 11],
    )
    runner.run(venue=venue, execution=_NoopExecution(), recorder=_RecorderWrapper())

    assert runner._pending_control_scheduling_obligation is None
    assert runner._next_send_ts_ns_local is None


def test_realized_old_deadline_does_not_runtime_pop_without_new_canonical_injection(
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
    def _spy_run_core_step(
        state: object,
        entry: object,
        *,
        configuration: object | None = None,
        control_time_queue_context: object | None = None,
        policy_admission_context: object | None = None,
        execution_control_apply_context: object | None = None,
    ) -> CoreStepResult:
        nonlocal control_count
        _ = (state, configuration)
        assert policy_admission_context is None
        assert execution_control_apply_context is None
        assert isinstance(control_time_queue_context, ControlTimeQueueReevaluationContext)
        if isinstance(entry.event, ControlTimeEvent):
            control_count += 1
        return CoreStepResult()

    monkeypatch.setattr(strategy_runner_module, "run_core_step", _spy_run_core_step)
    monkeypatch.setattr(
        runner.strategy_state,
        "pop_queued_intents",
        lambda _: (_ for _ in ()).throw(
            AssertionError("runtime must not pop control-time queue directly")
        ),
    )

    venue = _StubVenue(
        rc_sequence=[0, 0, 0, 1],
        ts_sequence=[1, 10, 10, 11],
    )
    runner.run(venue=venue, execution=_NoopExecution(), recorder=_RecorderWrapper())

    assert control_count == 1
    assert runner._event_stream_cursor.next_index == 1


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

    def _spy_run_core_step(
        state: object,
        entry: object,
        *,
        configuration: object | None = None,
        control_time_queue_context: object | None = None,
        policy_admission_context: object | None = None,
        execution_control_apply_context: object | None = None,
        core_decision_context: object | None = None,
        strategy_evaluator: object | None = None,
    ) -> CoreStepResult:
        _ = (state, configuration)
        _ = (core_decision_context, strategy_evaluator)
        assert policy_admission_context is None
        assert execution_control_apply_context is None
        positions.append((entry.position.index, type(entry.event).__name__))
        if isinstance(entry.event, MarketEvent):
            assert control_time_queue_context is None
            return CoreStepResult(generated_intents=(new_intent,))
        assert isinstance(control_time_queue_context, ControlTimeQueueReevaluationContext)
        return CoreStepResult()

    monkeypatch.setattr(strategy_runner_module, "process_event_entry", _spy_process_event_entry)
    monkeypatch.setattr(strategy_runner_module, "run_core_step", _spy_run_core_step)
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
    assert runner._event_stream_cursor.next_index == 3


def test_control_time_core_step_result_dispatches_via_existing_execution_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    control_intent = _new_intent(ts_ns_local=10)
    runner = HftStrategyRunner(
        engine_cfg=_engine_cfg(),
        strategy=_NoopStrategy(),
        risk_cfg=_risk_cfg(),
        core_cfg=_core_cfg(),
    )
    runner._next_send_ts_ns_local = 5

    obligation = _obligation(due_ts_ns_local=25, obligation_key="control-obligation")
    control_decision = _decision_for(
        [control_intent],
        next_send_ts_ns_local=25,
        control_scheduling_obligations=(obligation,),
    )

    callbacks: list[GateDecision] = []
    mark_calls: list[tuple[str, str, str]] = []
    submitted_events: list[OrderSubmittedEvent] = []

    def _spy_run_core_step(
        state: object,
        entry: object,
        *,
        configuration: object | None = None,
        control_time_queue_context: object | None = None,
        policy_admission_context: object | None = None,
        execution_control_apply_context: object | None = None,
    ) -> CoreStepResult:
        _ = (state, configuration)
        assert policy_admission_context is None
        assert execution_control_apply_context is None
        assert isinstance(control_time_queue_context, ControlTimeQueueReevaluationContext)
        return CoreStepResult(
            dispatchable_intents=(control_intent,),
            compat_gate_decision=control_decision,
            control_scheduling_obligation=obligation,
        )

    def _spy_process_event_entry(state: object, entry: object, *, configuration: object) -> None:
        _ = (state, configuration)
        if isinstance(entry.event, OrderSubmittedEvent):
            submitted_events.append(entry.event)

    monkeypatch.setattr(strategy_runner_module, "run_core_step", _spy_run_core_step)
    monkeypatch.setattr(strategy_runner_module, "process_event_entry", _spy_process_event_entry)
    monkeypatch.setattr(runner.strategy, "on_risk_decision", lambda d: callbacks.append(d))
    monkeypatch.setattr(
        runner.strategy_state,
        "mark_intent_sent",
        lambda i, c, t: mark_calls.append((i, c, t)),
    )
    monkeypatch.setattr(
        runner.risk,
        "decide_intents",
        lambda **_: (_ for _ in ()).throw(
            AssertionError("raw path risk gate should not run in this control-only wakeup")
        ),
    )

    venue = _StubVenue(
        rc_sequence=[0, 0, 1],
        ts_sequence=[1, 10, 11],
    )
    runner.run(venue=venue, execution=_NoopExecution(), recorder=_RecorderWrapper())

    assert len(submitted_events) == 1
    assert submitted_events[0].client_order_id == control_intent.client_order_id
    assert mark_calls == [(control_intent.instrument, control_intent.client_order_id, "new")]
    assert callbacks == [control_decision]
    assert runner._pending_control_scheduling_obligation == obligation
    assert runner._next_send_ts_ns_local == obligation.due_ts_ns_local


def test_same_wakeup_strategy_and_control_time_intents_are_processed_in_two_decisions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    strategy_intent = _new_intent(ts_ns_local=10)
    control_intent = _new_intent(ts_ns_local=10)
    control_intent = control_intent.model_copy(update={"client_order_id": "cid-control-1"})

    runner = HftStrategyRunner(
        engine_cfg=_engine_cfg(),
        strategy=_EmitIntentsStrategy([strategy_intent]),
        risk_cfg=_risk_cfg(),
        core_cfg=_core_cfg(),
    )
    runner._next_send_ts_ns_local = 5

    control_decision = _decision_for([control_intent])
    strategy_decision = _decision_for([strategy_intent])

    callback_order: list[str] = []

    class _ExecutionCapture:
        def __init__(self) -> None:
            self.batches: list[list[str]] = []

        def apply_intents(self, intents: list[Any]) -> list[tuple[Any, str]]:
            self.batches.append([it.client_order_id for it in intents])
            return []

    execution = _ExecutionCapture()

    def _spy_run_core_step(
        state: object,
        entry: object,
        *,
        configuration: object | None = None,
        control_time_queue_context: object | None = None,
        policy_admission_context: object | None = None,
        execution_control_apply_context: object | None = None,
        core_decision_context: object | None = None,
        strategy_evaluator: object | None = None,
    ) -> CoreStepResult:
        _ = (state, entry, configuration)
        _ = (core_decision_context, strategy_evaluator)
        assert policy_admission_context is None
        assert execution_control_apply_context is None
        if isinstance(entry.event, MarketEvent):
            assert control_time_queue_context is None
            return CoreStepResult(generated_intents=(strategy_intent,))
        assert isinstance(control_time_queue_context, ControlTimeQueueReevaluationContext)
        return CoreStepResult(
            dispatchable_intents=(control_intent,),
            compat_gate_decision=control_decision,
        )

    monkeypatch.setattr(strategy_runner_module, "run_core_step", _spy_run_core_step)
    monkeypatch.setattr(
        runner.risk,
        "decide_intents",
        lambda **_: strategy_decision,
    )
    monkeypatch.setattr(
        runner.strategy,
        "on_risk_decision",
        lambda decision: callback_order.append(
            "control"
            if decision is control_decision
            else "strategy"
        ),
    )

    venue = _StubVenue(
        rc_sequence=[0, 2, 1],
        ts_sequence=[1, 10, 11],
        depth=_depth_snapshot(),
    )
    runner.run(venue=venue, execution=execution, recorder=_RecorderWrapper())

    assert execution.batches == [
        [control_intent.client_order_id],
        [strategy_intent.client_order_id],
    ]
    assert callback_order == ["control", "strategy"]


@pytest.mark.parametrize(
    ("market_flag", "control_flag", "expected_dispatch_batches", "expected_raw_risk_calls"),
    [
        (
            False,
            False,
            [["cid-control-compat"], ["cid-strategy-gated"]],
            [["cid-strategy-generated"]],
        ),
        (
            True,
            False,
            [["cid-control-compat"], ["cid-market-core-step"]],
            [],
        ),
        (
            False,
            True,
            [["cid-control-core-step"], ["cid-strategy-gated"]],
            [["cid-strategy-generated"]],
        ),
        (
            True,
            True,
            [["cid-control-core-step"], ["cid-market-core-step"]],
            [],
        ),
    ],
    ids=(
        "market_off_control_off",
        "market_on_control_off",
        "market_off_control_on",
        "market_on_control_on",
    ),
)
def test_mixed_wakeup_matrix_characterization_keeps_split_dispatch_paths(
    monkeypatch: pytest.MonkeyPatch,
    market_flag: bool,
    control_flag: bool,
    expected_dispatch_batches: list[list[str]],
    expected_raw_risk_calls: list[list[str]],
) -> None:
    strategy_generated = _new_intent(ts_ns_local=10).model_copy(
        update={"client_order_id": "cid-strategy-generated"}
    )
    strategy_gated = _new_intent(ts_ns_local=10).model_copy(
        update={"client_order_id": "cid-strategy-gated"}
    )
    market_dispatchable = _new_intent(ts_ns_local=10).model_copy(
        update={"client_order_id": "cid-market-core-step"}
    )
    control_compat = _new_intent(ts_ns_local=10).model_copy(
        update={"client_order_id": "cid-control-compat"}
    )
    control_dispatchable = _new_intent(ts_ns_local=10).model_copy(
        update={"client_order_id": "cid-control-core-step"}
    )
    control_obligation = _obligation(due_ts_ns_local=25, obligation_key="control")

    runner = HftStrategyRunner(
        engine_cfg=_engine_cfg(),
        strategy=_EmitIntentsStrategy([strategy_generated]),
        risk_cfg=_risk_cfg(),
        core_cfg=_core_cfg(),
        enable_core_step_market_dispatch=market_flag,
        enable_core_step_control_time_dispatch=control_flag,
    )
    runner._next_send_ts_ns_local = 5
    seeded_core_step_errors = [(_new_intent(), "stale")]
    runner._last_core_step_execution_errors = list(seeded_core_step_errors)

    control_compat_decision = _decision_for(
        [control_compat],
        control_scheduling_obligations=(control_obligation,),
    )
    strategy_decision = _decision_for([strategy_gated])

    run_core_step_calls: list[dict[str, object]] = []
    callback_decisions: list[GateDecision] = []
    raw_risk_calls: list[list[str]] = []
    ordering: list[str] = []
    submitted_positions: list[tuple[int, str]] = []

    class _ExecutionCapture:
        def __init__(self) -> None:
            self.batches: list[list[str]] = []

        def apply_intents(self, intents: list[Any]) -> list[tuple[Any, str]]:
            self.batches.append([it.client_order_id for it in intents])
            return []

    execution = _ExecutionCapture()

    def _spy_run_core_step(
        state: object,
        entry: object,
        *,
        configuration: object | None = None,
        control_time_queue_context: object | None = None,
        policy_admission_context: object | None = None,
        execution_control_apply_context: object | None = None,
        core_decision_context: object | None = None,
        strategy_evaluator: object | None = None,
    ) -> CoreStepResult:
        _ = state
        _ = core_decision_context
        assert configuration is runner._core_cfg
        run_core_step_calls.append(
            {
                "event": type(entry.event).__name__,
                "position": entry.position.index,
                "strategy_evaluator": strategy_evaluator,
                "control_time_queue_context": control_time_queue_context,
                "policy_admission_context": policy_admission_context,
                "execution_control_apply_context": execution_control_apply_context,
            }
        )
        if isinstance(entry.event, MarketEvent):
            assert strategy_evaluator is not None
            if market_flag:
                assert isinstance(policy_admission_context, CorePolicyAdmissionContext)
                assert isinstance(
                    execution_control_apply_context,
                    CoreExecutionControlApplyContext,
                )
            else:
                assert policy_admission_context is None
                assert execution_control_apply_context is None
            assert control_time_queue_context is None
            return CoreStepResult(
                generated_intents=(strategy_generated,),
                dispatchable_intents=(
                    (market_dispatchable,) if market_flag else ()
                ),
            )
        assert isinstance(entry.event, ControlTimeEvent)
        assert strategy_evaluator is None
        if control_flag:
            assert control_time_queue_context is None
            assert isinstance(policy_admission_context, CorePolicyAdmissionContext)
            assert isinstance(
                execution_control_apply_context,
                CoreExecutionControlApplyContext,
            )
            return CoreStepResult(
                dispatchable_intents=(control_dispatchable,),
                compat_gate_decision=control_compat_decision,
                control_scheduling_obligation=control_obligation,
            )
        assert isinstance(control_time_queue_context, ControlTimeQueueReevaluationContext)
        assert policy_admission_context is None
        assert execution_control_apply_context is None
        return CoreStepResult(
            dispatchable_intents=(control_dispatchable,),
            compat_gate_decision=control_compat_decision,
            control_scheduling_obligation=control_obligation,
        )

    def _spy_decide_intents(**kwargs: object) -> GateDecision:
        raw_intents = kwargs["raw_intents"]
        raw_risk_calls.append([intent.client_order_id for intent in raw_intents])
        return strategy_decision

    def _spy_process_event_entry(state: object, entry: object, *, configuration: object) -> None:
        _ = (state, configuration)
        if isinstance(entry.event, OrderSubmittedEvent):
            ordering.append(f"submitted:{entry.event.client_order_id}")
            submitted_positions.append((entry.position.index, entry.event.client_order_id))

    def _spy_mark_intent_sent(instrument: str, client_order_id: str, intent_type: str) -> None:
        _ = (instrument, intent_type)
        ordering.append(f"mark:{client_order_id}")

    monkeypatch.setattr(strategy_runner_module, "run_core_step", _spy_run_core_step)
    monkeypatch.setattr(runner.risk, "decide_intents", _spy_decide_intents)
    monkeypatch.setattr(runner.strategy, "on_risk_decision", lambda decision: callback_decisions.append(decision))
    monkeypatch.setattr(strategy_runner_module, "process_event_entry", _spy_process_event_entry)
    monkeypatch.setattr(runner.strategy_state, "mark_intent_sent", _spy_mark_intent_sent)

    venue = _StubVenue(
        rc_sequence=[0, 2, 0, 1],
        ts_sequence=[1, 10, 10, 11],
        depth=_depth_snapshot(),
    )
    runner.run(venue=venue, execution=execution, recorder=_RecorderWrapper())

    assert [call["event"] for call in run_core_step_calls] == ["MarketEvent", "ControlTimeEvent"]
    assert [call["position"] for call in run_core_step_calls] == [0, 1]
    assert runner._last_injected_control_deadline_ns == 5
    assert runner._event_stream_cursor.next_index == 4

    # Dispatch remains split across market/control wakeups in all four flag combinations.
    assert execution.batches == expected_dispatch_batches
    assert raw_risk_calls == expected_raw_risk_calls

    if control_flag:
        assert control_compat_decision not in callback_decisions
    else:
        assert control_compat_decision in callback_decisions
    if market_flag:
        assert strategy_decision not in callback_decisions
    else:
        assert strategy_decision in callback_decisions

    submitted_ids = [client_order_id for _, client_order_id in submitted_positions]
    expected_submitted_ids = [batch[0] for batch in expected_dispatch_batches]
    assert submitted_ids == expected_submitted_ids
    assert [position for position, _ in submitted_positions] == [2, 3]
    assert ordering == [
        f"submitted:{expected_submitted_ids[0]}",
        f"mark:{expected_submitted_ids[0]}",
        f"submitted:{expected_submitted_ids[1]}",
        f"mark:{expected_submitted_ids[1]}",
    ]

    # _last_core_step_execution_errors is runtime-owned observability for the latest
    # CoreStep dispatch batch only. Legacy/compat paths must not mutate this field.
    if market_flag or control_flag:
        assert runner._last_core_step_execution_errors == []
    else:
        assert runner._last_core_step_execution_errors == seeded_core_step_errors

    assert runner._pending_control_scheduling_obligation is None
    assert runner._next_send_ts_ns_local is None


def test_control_time_core_step_mode_calls_run_core_step_with_policy_and_apply_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = HftStrategyRunner(
        engine_cfg=_engine_cfg(),
        strategy=_NoopStrategy(),
        risk_cfg=_risk_cfg_with_rate_limits(
            max_orders_per_second=5.0,
            max_cancels_per_second=2.0,
        ),
        core_cfg=_core_cfg(),
        enable_core_step_control_time_dispatch=True,
    )
    runner._next_send_ts_ns_local = 5
    captured: list[tuple[object, object, object, object]] = []

    def _spy_run_core_step(
        state: object,
        entry: object,
        *,
        configuration: object | None = None,
        control_time_queue_context: object | None = None,
        policy_admission_context: object | None = None,
        execution_control_apply_context: object | None = None,
        core_decision_context: object | None = None,
        strategy_evaluator: object | None = None,
    ) -> CoreStepResult:
        _ = core_decision_context
        assert control_time_queue_context is None
        assert strategy_evaluator is None
        captured.append(
            (
                state,
                configuration,
                policy_admission_context,
                execution_control_apply_context,
            )
        )
        return CoreStepResult()

    monkeypatch.setattr(strategy_runner_module, "run_core_step", _spy_run_core_step)
    monkeypatch.setattr(
        runner.risk,
        "decide_intents",
        lambda **_: (_ for _ in ()).throw(
            AssertionError("control-time core-step mode must not call runtime risk gate")
        ),
    )
    venue = _StubVenue(
        rc_sequence=[0, 0, 1],
        ts_sequence=[1, 10, 11],
    )
    runner.run(venue=venue, execution=_NoopExecution(), recorder=_RecorderWrapper())

    assert len(captured) == 1
    state, configuration, policy_ctx, apply_ctx = captured[0]
    assert state is runner.strategy_state
    assert configuration is runner._core_cfg
    assert isinstance(policy_ctx, CorePolicyAdmissionContext)
    assert policy_ctx.policy_evaluator is runner.risk
    assert policy_ctx.now_ts_ns_local == 10
    assert isinstance(apply_ctx, CoreExecutionControlApplyContext)
    assert apply_ctx.execution_control is runner.risk.execution_control
    assert apply_ctx.now_ts_ns_local == 10
    assert apply_ctx.max_orders_per_sec == 5.0
    assert apply_ctx.max_cancels_per_sec == 2.0
    assert apply_ctx.activate_dispatchable_outputs is True


def test_control_time_core_step_mode_dispatches_from_dispatchables_and_ignores_compat_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dispatchable = _new_intent(ts_ns_local=10)
    obligation = _obligation(due_ts_ns_local=25, obligation_key="control-core-step")
    runner = HftStrategyRunner(
        engine_cfg=_engine_cfg(),
        strategy=_NoopStrategy(),
        risk_cfg=_risk_cfg(),
        core_cfg=_core_cfg(),
        enable_core_step_control_time_dispatch=True,
    )
    runner._next_send_ts_ns_local = 5
    callbacks: list[GateDecision] = []

    class _ExecutionCapture:
        def __init__(self) -> None:
            self.batches: list[list[str]] = []

        def apply_intents(self, intents: list[Any]) -> list[tuple[Any, str]]:
            self.batches.append([it.client_order_id for it in intents])
            return []

    execution = _ExecutionCapture()
    compat_decision = _decision_for([])

    monkeypatch.setattr(
        strategy_runner_module,
        "run_core_step",
        lambda *args, **kwargs: CoreStepResult(
            dispatchable_intents=(dispatchable,),
            control_scheduling_obligation=obligation,
            compat_gate_decision=compat_decision,
        ),
    )
    monkeypatch.setattr(runner.strategy, "on_risk_decision", lambda d: callbacks.append(d))
    monkeypatch.setattr(
        runner.risk,
        "decide_intents",
        lambda **_: (_ for _ in ()).throw(
            AssertionError("control-time core-step mode must not call runtime risk gate")
        ),
    )
    venue = _StubVenue(
        rc_sequence=[0, 0, 1],
        ts_sequence=[1, 10, 11],
    )
    runner.run(venue=venue, execution=execution, recorder=_RecorderWrapper())

    assert execution.batches == [[dispatchable.client_order_id]]
    assert callbacks == []
    assert runner._pending_control_scheduling_obligation == obligation
    assert runner._next_send_ts_ns_local == obligation.due_ts_ns_local


def test_control_time_core_step_mode_none_obligation_clears_pending(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = HftStrategyRunner(
        engine_cfg=_engine_cfg(),
        strategy=_NoopStrategy(),
        risk_cfg=_risk_cfg(),
        core_cfg=_core_cfg(),
        enable_core_step_control_time_dispatch=True,
    )
    seeded = _obligation(due_ts_ns_local=5, obligation_key="seeded")
    runner._pending_control_scheduling_obligation = seeded
    runner._next_send_ts_ns_local = seeded.due_ts_ns_local

    monkeypatch.setattr(
        strategy_runner_module,
        "run_core_step",
        lambda *args, **kwargs: CoreStepResult(
            dispatchable_intents=(),
            control_scheduling_obligation=None,
        ),
    )
    venue = _StubVenue(
        rc_sequence=[0, 0, 1],
        ts_sequence=[1, 10, 11],
    )
    runner.run(venue=venue, execution=_NoopExecution(), recorder=_RecorderWrapper())

    assert runner._pending_control_scheduling_obligation is None
    assert runner._next_send_ts_ns_local is None


def test_control_time_core_step_mode_failure_preserves_pending_cursor_and_deadline_marker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = HftStrategyRunner(
        engine_cfg=_engine_cfg(),
        strategy=_NoopStrategy(),
        risk_cfg=_risk_cfg(),
        core_cfg=_core_cfg(),
        enable_core_step_control_time_dispatch=True,
    )
    obligation = _obligation(due_ts_ns_local=5, obligation_key="k-failure-core-step")
    runner._pending_control_scheduling_obligation = obligation
    runner._next_send_ts_ns_local = obligation.due_ts_ns_local

    monkeypatch.setattr(
        strategy_runner_module,
        "run_core_step",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            RuntimeError("boom-control-time-core-step")
        ),
    )
    venue = _StubVenue(
        rc_sequence=[0, 0, 1],
        ts_sequence=[1, 10, 11],
    )
    with pytest.raises(RuntimeError, match="boom-control-time-core-step"):
        runner.run(venue=venue, execution=_NoopExecution(), recorder=_RecorderWrapper())

    assert runner._last_injected_control_deadline_ns is None
    assert runner._pending_control_scheduling_obligation == obligation
    assert runner._next_send_ts_ns_local == obligation.due_ts_ns_local
    assert runner._event_stream_cursor.next_index == 0


def test_control_time_core_step_mode_same_deadline_injected_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = HftStrategyRunner(
        engine_cfg=_engine_cfg(),
        strategy=_NoopStrategy(),
        risk_cfg=_risk_cfg(),
        core_cfg=_core_cfg(),
        enable_core_step_control_time_dispatch=True,
    )
    runner._next_send_ts_ns_local = 5
    calls = {"count": 0}

    def _spy_run_core_step(*args: object, **kwargs: object) -> CoreStepResult:
        _ = (args, kwargs)
        calls["count"] += 1
        return CoreStepResult()

    monkeypatch.setattr(strategy_runner_module, "run_core_step", _spy_run_core_step)
    venue = _StubVenue(
        rc_sequence=[0, 0, 0, 1],
        ts_sequence=[1, 10, 10, 11],
    )
    runner.run(venue=venue, execution=_NoopExecution(), recorder=_RecorderWrapper())

    assert calls["count"] == 1


def test_control_time_core_step_mode_failed_dispatch_records_execution_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dispatchable_new = _new_intent(ts_ns_local=10)
    runner = HftStrategyRunner(
        engine_cfg=_engine_cfg(),
        strategy=_NoopStrategy(),
        risk_cfg=_risk_cfg(),
        core_cfg=_core_cfg(),
        enable_core_step_control_time_dispatch=True,
    )
    runner._next_send_ts_ns_local = 5
    submitted_event_count = 0
    marked_count = 0

    monkeypatch.setattr(
        strategy_runner_module,
        "run_core_step",
        lambda *args, **kwargs: CoreStepResult(
            dispatchable_intents=(dispatchable_new,),
        ),
    )

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

    class _ExecutionFailNew:
        def apply_intents(self, intents: list[Any]) -> list[tuple[Any, str]]:
            _ = intents
            return [(dispatchable_new, "EXCHANGE_REJECT")]

    venue = _StubVenue(
        rc_sequence=[0, 0, 1],
        ts_sequence=[1, 10, 11],
    )
    runner.run(
        venue=venue,
        execution=_ExecutionFailNew(),
        recorder=_RecorderWrapper(),
    )

    assert submitted_event_count == 0
    assert marked_count == 0
    assert len(runner._last_core_step_execution_errors) == 1
    failed_intent, failure_reason = runner._last_core_step_execution_errors[0]
    assert failed_intent.client_order_id == dispatchable_new.client_order_id
    assert failure_reason == "EXCHANGE_REJECT"


def test_wakeup_collapse_mixed_wakeup_uses_single_core_wakeup_call_and_single_dispatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    market_dispatchable = _new_intent(ts_ns_local=10).model_copy(
        update={"client_order_id": "cid-market-collapse"}
    )
    control_dispatchable = _new_intent(ts_ns_local=10).model_copy(
        update={"client_order_id": "cid-control-collapse"}
    )
    prior_pending = _obligation(due_ts_ns_local=5, obligation_key="pending")
    next_obligation = _obligation(due_ts_ns_local=25, obligation_key="next")
    runner = HftStrategyRunner(
        engine_cfg=_engine_cfg(),
        strategy=_EmitIntentsStrategy([_new_intent(ts_ns_local=10)]),
        risk_cfg=_risk_cfg(),
        core_cfg=_core_cfg(),
        enable_core_step_market_dispatch=True,
        enable_core_step_control_time_dispatch=True,
        enable_core_step_wakeup_collapse=True,
    )
    runner._pending_control_scheduling_obligation = prior_pending
    runner._next_send_ts_ns_local = prior_pending.due_ts_ns_local

    ordering: list[str] = []
    submitted_positions: list[tuple[int, str]] = []
    wakeup_calls: list[dict[str, object]] = []

    class _ExecutionCapture:
        def __init__(self) -> None:
            self.batches: list[list[str]] = []

        def apply_intents(self, intents: list[Any]) -> list[tuple[Any, str]]:
            self.batches.append([it.client_order_id for it in intents])
            return []

    execution = _ExecutionCapture()

    def _spy_run_core_wakeup_step(
        state: object,
        entries: tuple[object, ...],
        **kwargs: object,
    ) -> CoreStepResult:
        _ = state
        strategy_event_filter = kwargs["strategy_event_filter"]
        wakeup_calls.append(
            {
                "entries": tuple(type(entry.event).__name__ for entry in entries),
                "positions": tuple(entry.position.index for entry in entries),
                "strategy_event_filter_results": tuple(
                    strategy_event_filter(entry.event) for entry in entries
                ),
                "strategy_evaluator": kwargs["strategy_evaluator"],
                "snapshot_instrument": kwargs["snapshot_instrument"],
                "policy_admission_context": kwargs["policy_admission_context"],
                "execution_control_apply_context": kwargs["execution_control_apply_context"],
            }
        )
        return CoreStepResult(
            dispatchable_intents=(market_dispatchable, control_dispatchable),
            control_scheduling_obligation=next_obligation,
        )

    def _spy_process_event_entry(state: object, entry: object, *, configuration: object) -> None:
        _ = (state, configuration)
        if isinstance(entry.event, OrderSubmittedEvent):
            ordering.append(f"submitted:{entry.event.client_order_id}")
            submitted_positions.append((entry.position.index, entry.event.client_order_id))

    def _spy_mark_intent_sent(instrument: str, client_order_id: str, intent_type: str) -> None:
        _ = (instrument, intent_type)
        ordering.append(f"mark:{client_order_id}")

    monkeypatch.setattr(
        strategy_runner_module,
        "run_core_wakeup_step",
        _spy_run_core_wakeup_step,
    )
    monkeypatch.setattr(
        strategy_runner_module,
        "run_core_step",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("collapse mode must not call run_core_step for market/control path")
        ),
    )
    monkeypatch.setattr(
        runner.risk,
        "decide_intents",
        lambda **_: (_ for _ in ()).throw(
            AssertionError("collapse mode must not call runtime risk gate for market/control work")
        ),
    )
    monkeypatch.setattr(
        runner.strategy,
        "on_risk_decision",
        lambda *_: (_ for _ in ()).throw(
            AssertionError("collapse mode must not synthesize GateDecision callbacks")
        ),
    )
    monkeypatch.setattr(strategy_runner_module, "process_event_entry", _spy_process_event_entry)
    monkeypatch.setattr(runner.strategy_state, "mark_intent_sent", _spy_mark_intent_sent)

    venue = _StubVenue(
        rc_sequence=[0, 2, 0, 1],
        ts_sequence=[1, 10, 10, 11],
        depth=_depth_snapshot(),
    )
    runner.run(venue=venue, execution=execution, recorder=_RecorderWrapper())

    assert len(wakeup_calls) == 1
    wakeup_call = wakeup_calls[0]
    assert wakeup_call["entries"] == ("MarketEvent", "ControlTimeEvent")
    assert wakeup_call["positions"] == (0, 1)
    assert wakeup_call["strategy_event_filter_results"] == (True, False)
    assert wakeup_call["strategy_evaluator"] is not None
    assert wakeup_call["snapshot_instrument"] == "BTC_USDC-PERPETUAL"
    assert isinstance(wakeup_call["policy_admission_context"], CorePolicyAdmissionContext)
    assert isinstance(
        wakeup_call["execution_control_apply_context"],
        CoreExecutionControlApplyContext,
    )
    assert execution.batches == [[
        market_dispatchable.client_order_id,
        control_dispatchable.client_order_id,
    ]]
    assert [position for position, _ in submitted_positions] == [2, 3]
    assert ordering == [
        f"submitted:{market_dispatchable.client_order_id}",
        f"mark:{market_dispatchable.client_order_id}",
        f"submitted:{control_dispatchable.client_order_id}",
        f"mark:{control_dispatchable.client_order_id}",
    ]
    assert runner._last_core_step_execution_errors == []
    assert runner._pending_control_scheduling_obligation == next_obligation
    assert runner._next_send_ts_ns_local == next_obligation.due_ts_ns_local
    assert runner._last_injected_control_deadline_ns == 5
    assert runner._event_stream_cursor.next_index == 4


def test_wakeup_collapse_market_only_path_dispatches_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dispatchable = _new_intent(ts_ns_local=10).model_copy(
        update={"client_order_id": "cid-market-only-collapse"}
    )
    seeded = _obligation(due_ts_ns_local=99, obligation_key="seeded-market-only")
    runner = HftStrategyRunner(
        engine_cfg=_engine_cfg(),
        strategy=_NoopStrategy(),
        risk_cfg=_risk_cfg(),
        core_cfg=_core_cfg(),
        enable_core_step_market_dispatch=True,
        enable_core_step_control_time_dispatch=True,
        enable_core_step_wakeup_collapse=True,
    )
    runner._pending_control_scheduling_obligation = seeded
    runner._next_send_ts_ns_local = seeded.due_ts_ns_local
    calls: list[tuple[str, ...]] = []

    class _ExecutionCapture:
        def __init__(self) -> None:
            self.batches: list[list[str]] = []

        def apply_intents(self, intents: list[Any]) -> list[tuple[Any, str]]:
            self.batches.append([it.client_order_id for it in intents])
            return []

    execution = _ExecutionCapture()

    def _spy_run_core_wakeup_step(
        state: object,
        entries: tuple[object, ...],
        **kwargs: object,
    ) -> CoreStepResult:
        _ = (state, kwargs)
        calls.append(tuple(type(entry.event).__name__ for entry in entries))
        return CoreStepResult(
            dispatchable_intents=(dispatchable,),
            control_scheduling_obligation=None,
        )

    monkeypatch.setattr(strategy_runner_module, "run_core_wakeup_step", _spy_run_core_wakeup_step)
    monkeypatch.setattr(
        runner.risk,
        "decide_intents",
        lambda **_: (_ for _ in ()).throw(
            AssertionError("collapse market-only path must not call runtime risk gate")
        ),
    )

    venue = _StubVenue(
        rc_sequence=[0, 2, 1],
        ts_sequence=[1, 10, 11],
        depth=_depth_snapshot(),
    )
    runner.run(venue=venue, execution=execution, recorder=_RecorderWrapper())

    assert calls == [("MarketEvent",)]
    assert execution.batches == [[dispatchable.client_order_id]]
    assert runner._pending_control_scheduling_obligation is None
    assert runner._next_send_ts_ns_local is None


def test_wakeup_collapse_control_only_path_dispatches_once_without_strategy_eval(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dispatchable = _new_intent(ts_ns_local=10).model_copy(
        update={"client_order_id": "cid-control-only-collapse"}
    )
    pending = _obligation(due_ts_ns_local=5, obligation_key="pending-control-only")
    runner = HftStrategyRunner(
        engine_cfg=_engine_cfg(),
        strategy=_NoopStrategy(),
        risk_cfg=_risk_cfg(),
        core_cfg=_core_cfg(),
        enable_core_step_market_dispatch=True,
        enable_core_step_control_time_dispatch=True,
        enable_core_step_wakeup_collapse=True,
    )
    runner._pending_control_scheduling_obligation = pending
    runner._next_send_ts_ns_local = pending.due_ts_ns_local
    wakeup_calls: list[dict[str, object]] = []

    class _ExecutionCapture:
        def __init__(self) -> None:
            self.batches: list[list[str]] = []

        def apply_intents(self, intents: list[Any]) -> list[tuple[Any, str]]:
            self.batches.append([it.client_order_id for it in intents])
            return []

    execution = _ExecutionCapture()

    def _spy_run_core_wakeup_step(
        state: object,
        entries: tuple[object, ...],
        **kwargs: object,
    ) -> CoreStepResult:
        _ = state
        wakeup_calls.append(
            {
                "entries": tuple(type(entry.event).__name__ for entry in entries),
                "strategy_evaluator": kwargs["strategy_evaluator"],
            }
        )
        return CoreStepResult(
            dispatchable_intents=(dispatchable,),
            control_scheduling_obligation=None,
        )

    monkeypatch.setattr(strategy_runner_module, "run_core_wakeup_step", _spy_run_core_wakeup_step)
    monkeypatch.setattr(
        runner.risk,
        "decide_intents",
        lambda **_: (_ for _ in ()).throw(
            AssertionError("collapse control-only path must not call runtime risk gate")
        ),
    )

    venue = _StubVenue(
        rc_sequence=[0, 0, 1],
        ts_sequence=[1, 10, 11],
    )
    runner.run(venue=venue, execution=execution, recorder=_RecorderWrapper())

    assert len(wakeup_calls) == 1
    wakeup_call = wakeup_calls[0]
    assert wakeup_call["entries"] == ("ControlTimeEvent",)
    assert wakeup_call["strategy_evaluator"] is None
    assert execution.batches == [[dispatchable.client_order_id]]
    assert runner._pending_control_scheduling_obligation is None
    assert runner._next_send_ts_ns_local is None
    assert runner._last_injected_control_deadline_ns == 5


def test_wakeup_collapse_failure_before_result_preserves_pending_and_cursor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pending = _obligation(due_ts_ns_local=5, obligation_key="pending-failure")
    runner = HftStrategyRunner(
        engine_cfg=_engine_cfg(),
        strategy=_NoopStrategy(),
        risk_cfg=_risk_cfg(),
        core_cfg=_core_cfg(),
        enable_core_step_market_dispatch=True,
        enable_core_step_control_time_dispatch=True,
        enable_core_step_wakeup_collapse=True,
    )
    runner._pending_control_scheduling_obligation = pending
    runner._next_send_ts_ns_local = pending.due_ts_ns_local
    submitted_count = 0
    mark_count = 0

    monkeypatch.setattr(
        strategy_runner_module,
        "run_core_wakeup_step",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("boom-collapse")),
    )

    def _spy_process_event_entry(state: object, entry: object, *, configuration: object) -> None:
        nonlocal submitted_count
        _ = (state, configuration)
        if isinstance(entry.event, OrderSubmittedEvent):
            submitted_count += 1

    def _spy_mark(*args: object, **kwargs: object) -> None:
        nonlocal mark_count
        _ = (args, kwargs)
        mark_count += 1

    monkeypatch.setattr(strategy_runner_module, "process_event_entry", _spy_process_event_entry)
    monkeypatch.setattr(runner.strategy_state, "mark_intent_sent", _spy_mark)

    class _ExecutionMustNotRun:
        def apply_intents(self, intents: list[Any]) -> list[tuple[Any, str]]:
            _ = intents
            raise AssertionError("dispatch must not run after collapse wakeup failure")

    venue = _StubVenue(
        rc_sequence=[0, 2, 1],
        ts_sequence=[1, 10, 11],
        depth=_depth_snapshot(),
    )
    with pytest.raises(RuntimeError, match="boom-collapse"):
        runner.run(venue=venue, execution=_ExecutionMustNotRun(), recorder=_RecorderWrapper())

    assert submitted_count == 0
    assert mark_count == 0
    assert runner._pending_control_scheduling_obligation == pending
    assert runner._next_send_ts_ns_local == pending.due_ts_ns_local
    assert runner._last_injected_control_deadline_ns is None
    assert runner._event_stream_cursor.next_index == 0


def test_wakeup_collapse_same_deadline_injected_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = HftStrategyRunner(
        engine_cfg=_engine_cfg(),
        strategy=_NoopStrategy(),
        risk_cfg=_risk_cfg(),
        core_cfg=_core_cfg(),
        enable_core_step_market_dispatch=True,
        enable_core_step_control_time_dispatch=True,
        enable_core_step_wakeup_collapse=True,
    )
    runner._next_send_ts_ns_local = 5
    calls = {"count": 0}

    def _spy_run_core_wakeup_step(*args: object, **kwargs: object) -> CoreStepResult:
        _ = (args, kwargs)
        calls["count"] += 1
        return CoreStepResult()

    monkeypatch.setattr(
        strategy_runner_module,
        "run_core_wakeup_step",
        _spy_run_core_wakeup_step,
    )
    venue = _StubVenue(
        rc_sequence=[0, 0, 0, 1],
        ts_sequence=[1, 10, 10, 11],
    )
    runner.run(venue=venue, execution=_NoopExecution(), recorder=_RecorderWrapper())

    assert calls["count"] == 1


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
