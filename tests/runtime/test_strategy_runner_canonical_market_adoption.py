from __future__ import annotations

import inspect
from types import SimpleNamespace
from typing import Any

import pytest
from tradingchassis_core.core.domain.configuration import CoreConfiguration
from tradingchassis_core.core.domain.step_result import CoreStepResult
from tradingchassis_core.core.domain.types import (
    ControlTimeEvent,
    NewOrderIntent,
    Price,
    Quantity,
)
from tradingchassis_core.core.execution_control.types import ControlSchedulingObligation
from tradingchassis_core.core.risk.risk_config import RiskConfig

import core_runtime.backtest.engine.strategy_runner as strategy_runner_module
from core_runtime.backtest.engine.strategy_runner import HftStrategyRunner


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


def _risk_cfg() -> RiskConfig:
    return RiskConfig(
        scope="runtime-test",
        notional_limits={
            "currency": "USDC",
            "max_gross_notional": 10_000.0,
            "max_single_order_notional": 1_000.0,
        },
        order_rate_limits={
            "max_orders_per_second": 10.0,
            "max_cancels_per_second": 10.0,
        },
    )


def _engine_cfg() -> Any:
    return SimpleNamespace(
        instrument="BTC_USDC-PERPETUAL",
        max_price_tick_levels=2,
        event_bus_path="/tmp/runtime-events.jsonl",
        tick_size=0.1,
    )


def _new_intent(*, ts_ns_local: int, client_order_id: str) -> NewOrderIntent:
    return NewOrderIntent(
        ts_ns_local=ts_ns_local,
        instrument="BTC_USDC-PERPETUAL",
        client_order_id=client_order_id,
        intent_type="new",
        order_type="limit",
        side="buy",
        intended_price=Price(currency="UNKNOWN", value=100.0),
        intended_qty=Quantity(value=0.1, unit="contracts"),
        time_in_force="GTC",
    )


class _Strategy:
    def __init__(self) -> None:
        self.on_feed_calls = 0
        self.on_order_update_calls = 0

    def on_feed(self, state: Any, event: Any, engine_cfg: Any, constraints: Any) -> list[Any]:
        _ = (state, event, engine_cfg, constraints)
        self.on_feed_calls += 1
        return []

    def on_order_update(self, state: Any, engine_cfg: Any, constraints: Any) -> list[Any]:
        _ = (state, engine_cfg, constraints)
        self.on_order_update_calls += 1
        return []


class _Execution:
    def __init__(self) -> None:
        self.applied: list[list[Any]] = []

    def apply_intents(self, intents: list[Any]) -> list[tuple[Any, str]]:
        self.applied.append(list(intents))
        return []


class _Recorder:
    def __init__(self) -> None:
        self.recorder = SimpleNamespace(record=lambda _hbt: None)


class _Venue:
    def __init__(self, *, rc_sequence: list[int], ts_sequence: list[int]) -> None:
        self._rc = list(rc_sequence)
        self._ts = list(ts_sequence)
        self._now = 0

    def wait_next(self, *, timeout_ns: int, include_order_resp: bool) -> int:
        _ = (timeout_ns, include_order_resp)
        self._now = self._ts.pop(0)
        return self._rc.pop(0)

    def current_timestamp_ns(self) -> int:
        return self._now

    def read_market_snapshot(self) -> Any:
        return SimpleNamespace(
            roi_lb_tick=1000,
            tick_size=0.1,
            best_bid_tick=1005,
            best_ask_tick=1006,
            bid_depth=[1.0, 0.9, 0.8],
            ask_depth=[1.1, 1.2, 1.3],
        )

    def read_orders_snapshot(self) -> tuple[Any, Any]:
        return (
            SimpleNamespace(
                position=1.0,
                balance=1000.0,
                fee=1.5,
                trading_volume=100.0,
                trading_value=5000.0,
                num_trades=3,
            ),
            SimpleNamespace(values=lambda: []),
        )

    def record(self, recorder: Any) -> None:
        recorder.recorder.record(self)


def _runner(**kwargs: Any) -> HftStrategyRunner:
    return HftStrategyRunner(
        engine_cfg=_engine_cfg(),
        strategy=_Strategy(),
        risk_cfg=_risk_cfg(),
        core_cfg=_core_cfg(),
        **kwargs,
    )


def test_market_path_dispatches_only_from_core_step_result(monkeypatch: Any) -> None:
    runner = _runner()
    execution = _Execution()
    venue = _Venue(rc_sequence=[0, 2, 1], ts_sequence=[1, 2, 3])
    recorder = _Recorder()

    captured_kwargs: list[dict[str, Any]] = []

    def _stub_run_core_step(state: Any, entry: Any, **kwargs: Any) -> CoreStepResult:
        _ = (state, entry)
        captured_kwargs.append(kwargs)
        return CoreStepResult(
            generated_intents=(_new_intent(ts_ns_local=2, client_order_id="generated"),),
            dispatchable_intents=(_new_intent(ts_ns_local=2, client_order_id="dispatch"),),
        )

    monkeypatch.setattr(strategy_runner_module, "run_core_step", _stub_run_core_step)

    def _fail_decide_intents(**_: Any) -> Any:
        raise AssertionError("runtime must not call risk.decide_intents")

    monkeypatch.setattr(runner.risk, "decide_intents", _fail_decide_intents, raising=False)

    runner.run(venue, execution, recorder)

    assert len(execution.applied) == 1
    assert [it.client_order_id for it in execution.applied[0]] == ["dispatch"]
    assert "policy_admission_context" in captured_kwargs[0]
    assert "execution_control_apply_context" in captured_kwargs[0]


def test_control_time_path_uses_core_step_dispatchable_intents(monkeypatch: Any) -> None:
    runner = _runner()
    execution = _Execution()
    venue = _Venue(rc_sequence=[0, 0, 1], ts_sequence=[1, 2, 3])
    recorder = _Recorder()
    runner._pending_control_scheduling_obligation = ControlSchedulingObligation(
        due_ts_ns_local=2,
        reason="rate_limit",
        scope_key="instrument:BTC_USDC-PERPETUAL",
        source="test",
    )
    runner._next_send_ts_ns_local = 2

    captured: list[tuple[Any, dict[str, Any]]] = []

    def _stub_run_core_step(state: Any, entry: Any, **kwargs: Any) -> CoreStepResult:
        _ = state
        captured.append((entry.event, kwargs))
        if isinstance(entry.event, ControlTimeEvent):
            return CoreStepResult(
                dispatchable_intents=(_new_intent(ts_ns_local=2, client_order_id="ctl"),)
            )
        return CoreStepResult()

    monkeypatch.setattr(strategy_runner_module, "run_core_step", _stub_run_core_step)
    runner.run(venue, execution, recorder)

    assert [it.client_order_id for it in execution.applied[0]] == ["ctl"]
    assert runner._pending_control_scheduling_obligation is None
    control_events = [event for event, _ in captured if isinstance(event, ControlTimeEvent)]
    assert len(control_events) == 1
    control_kwargs = [kwargs for event, kwargs in captured if isinstance(event, ControlTimeEvent)][0]
    assert "control_time_queue_context" not in control_kwargs


def test_mixed_wakeup_path_uses_new_wakeup_evaluator_api(monkeypatch: Any) -> None:
    runner = _runner(enable_core_step_wakeup_collapse=True)
    execution = _Execution()
    venue = _Venue(rc_sequence=[0, 2, 1], ts_sequence=[1, 2, 3])
    recorder = _Recorder()
    runner._pending_control_scheduling_obligation = ControlSchedulingObligation(
        due_ts_ns_local=2,
        reason="rate_limit",
        scope_key="instrument:BTC_USDC-PERPETUAL",
        source="test",
    )
    runner._next_send_ts_ns_local = 2

    observed: dict[str, Any] = {}

    def _stub_run_core_wakeup_step(state: Any, entries: Any, **kwargs: Any) -> CoreStepResult:
        observed["entry_count"] = len(entries)
        observed["kwargs"] = kwargs
        evaluator = kwargs["wakeup_strategy_evaluator"]
        if evaluator is not None:
            evaluator.evaluate(SimpleNamespace(state=state, entries=entries))
        return CoreStepResult()

    monkeypatch.setattr(strategy_runner_module, "run_core_wakeup_step", _stub_run_core_wakeup_step)
    runner.run(venue, execution, recorder)

    assert observed["entry_count"] == 2  # market + injected control-time
    assert "wakeup_strategy_evaluator" in observed["kwargs"]
    assert "strategy_event_filter" not in observed["kwargs"]
    assert observed["kwargs"]["queued_instrument"] == "BTC_USDC-PERPETUAL"
    assert runner.strategy.on_feed_calls == 1


def test_rc3_feedback_event_uses_account_level_shape(monkeypatch: Any) -> None:
    runner = _runner()
    execution = _Execution()
    venue = _Venue(rc_sequence=[0, 3, 1], ts_sequence=[1, 2, 3])
    recorder = _Recorder()

    seen_event_payloads: list[dict[str, Any]] = []

    def _stub_run_core_step(state: Any, entry: Any, **kwargs: Any) -> CoreStepResult:
        _ = (state, kwargs)
        if hasattr(entry.event, "ts_ns_local_feedback"):
            seen_event_payloads.append(entry.event.model_dump())
        return CoreStepResult()

    monkeypatch.setattr(strategy_runner_module, "run_core_step", _stub_run_core_step)
    runner.run(venue, execution, recorder)

    assert len(seen_event_payloads) == 1
    assert "order_snapshots" not in seen_event_payloads[0]


def test_order_submitted_event_emitted_before_mark_intent_sent(monkeypatch: Any) -> None:
    runner = _runner()
    execution = _Execution()
    call_order: list[str] = []

    def _spy_process_submitted(intent: Any, *, ts_ns_local_dispatch: int) -> None:
        _ = (intent, ts_ns_local_dispatch)
        call_order.append("submitted")

    def _spy_mark_sent(instrument: str, client_order_id: str, intent_type: str) -> None:
        _ = (instrument, client_order_id, intent_type)
        call_order.append("mark_sent")

    monkeypatch.setattr(runner, "_process_canonical_order_submitted_event", _spy_process_submitted)
    monkeypatch.setattr(runner.strategy_state, "mark_intent_sent", _spy_mark_sent)

    runner._dispatch_accepted_intents(
        [_new_intent(ts_ns_local=2, client_order_id="new-1")],
        execution,
        sim_now_ns=2,
    )

    assert call_order == ["submitted"]


def test_successful_new_dispatch_does_not_leave_stale_inflight() -> None:
    runner = _runner()
    execution = _Execution()

    runner._dispatch_accepted_intents(
        [_new_intent(ts_ns_local=2, client_order_id="new-1")],
        execution,
        sim_now_ns=2,
    )

    assert runner.strategy_state.has_inflight("BTC_USDC-PERPETUAL", "new-1") is False


def test_runner_source_has_no_removed_compat_api_usage() -> None:
    source = inspect.getsource(strategy_runner_module)
    assert "decide_intents" not in source
    assert "compat_gate_decision" not in source
    assert "ControlTimeQueueReevaluationContext" not in source
    assert "strategy_event_filter" not in source


def test_stale_control_obligation_is_cleared_to_avoid_zero_timeout_spin() -> None:
    runner = _runner()
    execution = _Execution()
    recorder = _Recorder()

    class _TimeoutSensitiveVenue:
        def __init__(self) -> None:
            self._now = 0
            self._call_count = 0

        def wait_next(self, *, timeout_ns: int, include_order_resp: bool) -> int:
            _ = include_order_resp
            self._call_count += 1
            if self._call_count == 1:
                self._now = 1
                return 0
            if timeout_ns == 0:
                if self._call_count > 20:
                    raise AssertionError("runner is spinning on zero-timeout wait_next")
                return 0
            self._now = 2
            return 1

        def current_timestamp_ns(self) -> int:
            return self._now

        def read_market_snapshot(self) -> Any:
            raise AssertionError("market snapshot should not be needed")

        def read_orders_snapshot(self) -> tuple[Any, Any]:
            raise AssertionError("orders snapshot should not be needed")

        def record(self, recorder: Any) -> None:
            recorder.recorder.record(self)

    venue = _TimeoutSensitiveVenue()
    runner._pending_control_scheduling_obligation = ControlSchedulingObligation(
        due_ts_ns_local=1,
        reason="rate_limit",
        scope_key="instrument:BTC_USDC-PERPETUAL",
        source="test",
    )
    runner._next_send_ts_ns_local = 1
    runner._last_injected_control_deadline_ns = 1

    runner.run(venue, execution, recorder)

    assert runner._pending_control_scheduling_obligation is None
    assert runner._next_send_ts_ns_local is None


def test_debug_max_iterations_guard_raises_with_loop_state(monkeypatch: Any) -> None:
    runner = _runner()
    execution = _Execution()
    recorder = _Recorder()
    runner._pending_control_scheduling_obligation = ControlSchedulingObligation(
        due_ts_ns_local=1_000_000_000_000,
        reason="rate_limit",
        scope_key="instrument:BTC_USDC-PERPETUAL",
        source="test",
    )
    runner._next_send_ts_ns_local = 1_000_000_000_000

    class _NeverEndingVenue:
        def __init__(self) -> None:
            self._now = 1

        def wait_next(self, *, timeout_ns: int, include_order_resp: bool) -> int:
            _ = (timeout_ns, include_order_resp)
            return 0

        def current_timestamp_ns(self) -> int:
            return self._now

        def read_market_snapshot(self) -> Any:
            raise AssertionError("market snapshot should not be needed")

        def read_orders_snapshot(self) -> tuple[Any, Any]:
            raise AssertionError("orders snapshot should not be needed")

        def record(self, recorder: Any) -> None:
            recorder.recorder.record(self)

    monkeypatch.setenv("TRADINGCHASSIS_DEBUG_MAX_ITERATIONS", "5")

    with pytest.raises(RuntimeError, match="TRADINGCHASSIS_DEBUG_MAX_ITERATIONS exceeded"):
        runner.run(_NeverEndingVenue(), execution, recorder)


def test_runner_terminates_on_no_work_no_progress_loop() -> None:
    runner = _runner()
    execution = _Execution()
    recorder = _Recorder()

    class _NoProgressVenue:
        def __init__(self) -> None:
            self._now = 100
            self.wait_call_count = 0
            self.record_call_count = 0

        def wait_next(self, *, timeout_ns: int, include_order_resp: bool) -> int:
            _ = (timeout_ns, include_order_resp)
            self.wait_call_count += 1
            if self.wait_call_count == 1:
                self._now = 100
                return 0
            if self.wait_call_count > 3:
                raise AssertionError("runner should have terminated no-work/no-progress loop")
            return 0

        def current_timestamp_ns(self) -> int:
            return self._now

        def read_market_snapshot(self) -> Any:
            raise AssertionError("market snapshot should not be needed")

        def read_orders_snapshot(self) -> tuple[Any, Any]:
            raise AssertionError("orders snapshot should not be needed")

        def record(self, _recorder: Any) -> bool:
            self.record_call_count += 1
            return False

    venue = _NoProgressVenue()
    runner.run(venue, execution, recorder)

    assert venue.wait_call_count == 2  # init wait + one loop iteration
    assert venue.record_call_count == 0


def test_runner_terminates_on_no_work_with_non_event_rc() -> None:
    runner = _runner()
    execution = _Execution()
    recorder = _Recorder()

    class _Rc14Venue:
        def __init__(self) -> None:
            self._now = 100
            self.wait_call_count = 0
            self.record_call_count = 0

        def wait_next(self, *, timeout_ns: int, include_order_resp: bool) -> int:
            _ = (timeout_ns, include_order_resp)
            self.wait_call_count += 1
            if self.wait_call_count == 1:
                return 0
            if self.wait_call_count > 3:
                raise AssertionError("runner should terminate on non-event rc with no work")
            return 14

        def current_timestamp_ns(self) -> int:
            return self._now

        def read_market_snapshot(self) -> Any:
            raise AssertionError("market snapshot should not be needed")

        def read_orders_snapshot(self) -> tuple[Any, Any]:
            raise AssertionError("orders snapshot should not be needed")

        def record(self, _recorder: Any) -> bool:
            self.record_call_count += 1
            return False

    venue = _Rc14Venue()
    runner.run(venue, execution, recorder)

    assert venue.wait_call_count == 2
    assert venue.record_call_count == 0


def test_runner_does_not_terminate_early_when_pending_work_exists() -> None:
    runner = _runner()
    execution = _Execution()
    recorder = _Recorder()
    runner._pending_control_scheduling_obligation = ControlSchedulingObligation(
        due_ts_ns_local=200,
        reason="rate_limit",
        scope_key="instrument:BTC_USDC-PERPETUAL",
        source="test",
    )
    runner._next_send_ts_ns_local = 200

    class _PendingAwareVenue:
        def __init__(self) -> None:
            self._now = 100
            self.wait_call_count = 0

        def wait_next(self, *, timeout_ns: int, include_order_resp: bool) -> int:
            _ = include_order_resp
            self.wait_call_count += 1
            if self.wait_call_count == 1:
                self._now = 100
                return 0
            if self.wait_call_count == 2:
                assert timeout_ns > 0
                self._now = 100
                return 0
            self._now = 101
            return 1

        def current_timestamp_ns(self) -> int:
            return self._now

        def read_market_snapshot(self) -> Any:
            raise AssertionError("market snapshot should not be needed")

        def read_orders_snapshot(self) -> tuple[Any, Any]:
            raise AssertionError("orders snapshot should not be needed")

        def record(self, _recorder: Any) -> bool:
            return False

    venue = _PendingAwareVenue()
    runner.run(venue, execution, recorder)

    assert venue.wait_call_count == 3
