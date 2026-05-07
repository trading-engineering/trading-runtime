"""Strategy execution loop for HFT backtests."""

from __future__ import annotations

import logging
from collections import deque
from pathlib import Path
from typing import TYPE_CHECKING, Any

from tradingchassis_core import (
    ControlTimeQueueReevaluationContext,
    CoreExecutionControlApplyContext,
    CorePolicyAdmissionContext,
    run_core_step,
    run_core_wakeup_step,
)
from tradingchassis_core.core.domain.configuration import CoreConfiguration
from tradingchassis_core.core.domain.processing import process_event_entry
from tradingchassis_core.core.domain.processing_order import (
    EventStreamEntry,
)
from tradingchassis_core.core.domain.state import StrategyState
from tradingchassis_core.core.domain.step_result import CoreStepResult
from tradingchassis_core.core.domain.types import (
    BookLevel,
    BookPayload,
    ControlTimeEvent,
    MarketEvent,
    NewOrderIntent,
    OrderIntent,
    OrderSubmittedEvent,
    Price,
    Quantity,
)
from tradingchassis_core.core.events.event_bus import EventBus
from tradingchassis_core.core.events.sinks.sink_logging import LoggingEventSink
from tradingchassis_core.core.execution_control.types import (
    ControlSchedulingObligation,
)
from tradingchassis_core.core.ports.venue_adapter import VenueAdapter
from tradingchassis_core.core.risk.risk_config import RiskConfig
from tradingchassis_core.core.risk.risk_engine import (
    GateDecision,
    RejectedIntent,
    RiskEngine,
)

from core_runtime.backtest.adapters.protocols import OrderSubmissionGateway
from core_runtime.backtest.engine.event_stream_cursor import EventStreamCursor
from core_runtime.core.events.sinks.file_recorder import FileRecorderSink

if TYPE_CHECKING:
    from tradingchassis_core.strategies.base import Strategy

    from core_runtime.backtest.engine.hft_engine import HftEngineConfig


MAX_TIMEOUT_NS = 1 << 62  # Effectively "wait forever" without a heartbeat


class _LegacyOnFeedStrategyEvaluator:
    """Runtime-local adapter from legacy Strategy.on_feed to CoreStep evaluator."""

    def __init__(
        self,
        *,
        strategy: Strategy,
        engine_cfg: HftEngineConfig,
        constraints: object,
    ) -> None:
        self._strategy = strategy
        self._engine_cfg = engine_cfg
        self._constraints = constraints

    def evaluate(self, context: object) -> tuple[OrderIntent, ...]:
        return tuple(
            self._strategy.on_feed(
                context.state,
                context.event,
                self._engine_cfg,
                self._constraints,
            )
        )


class HftStrategyRunner:
    """Strategy runner for HFT backtests.

    Invariant:
    - One wait_next() wakeup corresponds to one fully committed timestamp block.
    - Strategy is evaluated at most once per wakeup on a stable state.
    """
    # pylint: disable=too-many-instance-attributes

    def __init__(
        self,
        *,
        engine_cfg: HftEngineConfig,
        strategy: Strategy,
        risk_cfg: RiskConfig,
        core_cfg: CoreConfiguration,
        enable_core_step_market_dispatch: bool = False,
        enable_core_step_control_time_dispatch: bool = False,
        enable_core_step_wakeup_collapse: bool = False,
    ) -> None:
        self.engine_cfg = engine_cfg
        self.strategy = strategy
        self._core_cfg = core_cfg
        self._enable_core_step_market_dispatch = enable_core_step_market_dispatch
        self._enable_core_step_control_time_dispatch = (
            enable_core_step_control_time_dispatch
        )
        self._enable_core_step_wakeup_collapse = enable_core_step_wakeup_collapse
        if self._enable_core_step_wakeup_collapse and not self._enable_core_step_market_dispatch:
            raise ValueError(
                "enable_core_step_wakeup_collapse=True requires "
                "enable_core_step_market_dispatch=True"
            )
        if (
            self._enable_core_step_wakeup_collapse
            and not self._enable_core_step_control_time_dispatch
        ):
            raise ValueError(
                "enable_core_step_wakeup_collapse=True requires "
                "enable_core_step_control_time_dispatch=True"
            )

        event_bus = self._build_event_bus(
            path=Path(engine_cfg.event_bus_path),
        )

        self.strategy_state = StrategyState(
            event_bus=event_bus,
        )

        self.risk = RiskEngine(
            risk_cfg=risk_cfg,
            event_bus=event_bus,
        )

        self._next_send_ts_ns_local: int | None = None
        self._event_stream_cursor = EventStreamCursor()
        self._last_injected_control_deadline_ns: int | None = None
        self._pending_control_scheduling_obligation: ControlSchedulingObligation | None = None
        self._last_core_step_execution_errors: list[tuple[OrderIntent, str]] = []

    def _process_canonical_event(self, event: object) -> None:
        position = self._event_stream_cursor.attempt_position()
        entry = EventStreamEntry(
            position=position,
            event=event,
        )
        process_event_entry(
            self.strategy_state,
            entry,
            configuration=self._core_cfg,
        )
        self._event_stream_cursor.commit_success(position)

    def _build_event_bus(
        self,
        *,
        path: Path,
    ) -> EventBus:
        logger = logging.getLogger("bus")

        sinks = [
            LoggingEventSink(logger),
            FileRecorderSink(path),
        ]

        return EventBus(sinks=sinks)

    def _close_event_bus(self) -> None:
        self.strategy_state._event_bus.close()
        self.risk._event_bus.close()

    def _compute_timeout_ns(self, now_local_ns: int) -> int:
        """Compute wait timeout in nanoseconds."""
        if self._next_send_ts_ns_local is None:
            return MAX_TIMEOUT_NS
        delta = self._next_send_ts_ns_local - now_local_ns
        return 0 if delta <= 0 else delta

    def _sort_intents_for_gate(self, intents: list[OrderIntent]) -> list[OrderIntent]:
        """Sort intents to ensure cancels are evaluated first."""

        def intent_priority(intent: OrderIntent) -> int:
            if intent.intent_type == "cancel":
                return 0
            if intent.intent_type == "replace":
                return 1
            if intent.intent_type == "new":
                return 2
            return 9

        return sorted(intents, key=lambda it: (intent_priority(it), it.ts_ns_local))

    def _build_policy_and_apply_context(
        self,
        *,
        now_ts_ns_local: int,
    ) -> tuple[CorePolicyAdmissionContext, CoreExecutionControlApplyContext]:
        rate_cfg = self.risk.risk_cfg.order_rate_limits
        max_orders_per_sec = (
            None if rate_cfg is None else rate_cfg.max_orders_per_second
        )
        max_cancels_per_sec = (
            None if rate_cfg is None else rate_cfg.max_cancels_per_second
        )
        return (
            CorePolicyAdmissionContext(
                policy_evaluator=self.risk,
                now_ts_ns_local=now_ts_ns_local,
            ),
            CoreExecutionControlApplyContext(
                execution_control=self.risk.execution_control,
                now_ts_ns_local=now_ts_ns_local,
                max_orders_per_sec=max_orders_per_sec,
                max_cancels_per_sec=max_cancels_per_sec,
                activate_dispatchable_outputs=True,
            ),
        )

    def _process_canonical_market_event(
        self,
        market_event: MarketEvent,
        *,
        constraints: object,
    ) -> CoreStepResult:
        position = self._event_stream_cursor.attempt_position()
        entry = EventStreamEntry(
            position=position,
            event=market_event,
        )
        policy_admission_context = None
        execution_control_apply_context = None
        if getattr(self, "_enable_core_step_market_dispatch", False):
            (
                policy_admission_context,
                execution_control_apply_context,
            ) = self._build_policy_and_apply_context(
                now_ts_ns_local=market_event.ts_ns_local,
            )
        run_core_step_kwargs: dict[str, object] = {
            "configuration": self._core_cfg,
            "strategy_evaluator": _LegacyOnFeedStrategyEvaluator(
                strategy=self.strategy,
                engine_cfg=self.engine_cfg,
                constraints=constraints,
            ),
        }
        if policy_admission_context is not None:
            run_core_step_kwargs["policy_admission_context"] = policy_admission_context
        if execution_control_apply_context is not None:
            run_core_step_kwargs["execution_control_apply_context"] = (
                execution_control_apply_context
            )
        result = run_core_step(
            self.strategy_state,
            entry,
            **run_core_step_kwargs,
        )
        self._event_stream_cursor.commit_success(position)
        return result

    def _process_canonical_order_submitted_event(
        self,
        intent: NewOrderIntent,
        *,
        ts_ns_local_dispatch: int,
    ) -> None:
        order_submitted_event = OrderSubmittedEvent(
            ts_ns_local_dispatch=ts_ns_local_dispatch,
            instrument=intent.instrument,
            client_order_id=intent.client_order_id,
            side=intent.side,
            order_type=intent.order_type,
            intended_price=intent.intended_price,
            intended_qty=intent.intended_qty,
            time_in_force=intent.time_in_force,
            intent_correlation_id=intent.intents_correlation_id,
            dispatch_attempt_id=None,
            runtime_correlation=None,
        )
        self._process_canonical_event(order_submitted_event)

    def _process_canonical_control_time_event(
        self,
        *,
        instrument: str,
        now_ts_ns_local: int,
        sim_now_ns: int,
        scheduled_deadline_ns: int,
        scheduling_obligation: ControlSchedulingObligation | None = None,
    ) -> CoreStepResult:
        control_time_event = self._build_control_time_event(
            sim_now_ns=sim_now_ns,
            scheduled_deadline_ns=scheduled_deadline_ns,
            scheduling_obligation=scheduling_obligation,
        )
        position = self._event_stream_cursor.attempt_position()
        entry = EventStreamEntry(
            position=position,
            event=control_time_event,
        )
        run_core_step_kwargs: dict[str, object] = {
            "configuration": self._core_cfg,
        }
        if getattr(self, "_enable_core_step_control_time_dispatch", False):
            (
                policy_admission_context,
                execution_control_apply_context,
            ) = self._build_policy_and_apply_context(
                now_ts_ns_local=now_ts_ns_local,
            )
            run_core_step_kwargs["policy_admission_context"] = (
                policy_admission_context
            )
            run_core_step_kwargs["execution_control_apply_context"] = (
                execution_control_apply_context
            )
        else:
            run_core_step_kwargs["control_time_queue_context"] = (
                ControlTimeQueueReevaluationContext(
                    risk_engine=self.risk,
                    instrument=instrument,
                    now_ts_ns_local=now_ts_ns_local,
                )
            )
        result = run_core_step(
            self.strategy_state,
            entry,
            **run_core_step_kwargs,
        )
        self._event_stream_cursor.commit_success(position)
        return result

    def _build_control_time_event(
        self,
        *,
        sim_now_ns: int,
        scheduled_deadline_ns: int,
        scheduling_obligation: ControlSchedulingObligation | None = None,
    ) -> ControlTimeEvent:
        obligation_reason = "rate_limit"
        obligation_due_ts_ns_local = scheduled_deadline_ns
        if scheduling_obligation is not None:
            obligation_reason = scheduling_obligation.reason
            obligation_due_ts_ns_local = scheduling_obligation.due_ts_ns_local
        return ControlTimeEvent(
            ts_ns_local_control=sim_now_ns,
            reason="scheduled_control_recheck",
            due_ts_ns_local=scheduled_deadline_ns,
            realized_ts_ns_local=sim_now_ns,
            obligation_reason=obligation_reason,
            obligation_due_ts_ns_local=obligation_due_ts_ns_local,
            runtime_correlation=None,
        )

    def _allocate_wakeup_entries(
        self,
        events: list[object],
    ) -> tuple[EventStreamEntry, ...]:
        positions = self._event_stream_cursor.attempt_positions(len(events))
        return tuple(
            EventStreamEntry(
                position=position,
                event=event,
            )
            for position, event in zip(positions, events, strict=True)
        )

    def _commit_wakeup_entries(self, entries: tuple[EventStreamEntry, ...]) -> None:
        for entry in entries:
            self._event_stream_cursor.commit_success(entry.position)

    @staticmethod
    def _select_effective_control_scheduling_obligation(
        decision: GateDecision,
    ) -> ControlSchedulingObligation | None:
        obligations = decision.control_scheduling_obligations
        if not obligations:
            return None
        return min(
            obligations,
            key=lambda obligation: (
                obligation.due_ts_ns_local,
                obligation.obligation_key,
            ),
        )

    def _clear_pending_control_scheduling_obligation(self) -> None:
        self._pending_control_scheduling_obligation = None
        self._next_send_ts_ns_local = None

    def _consume_pending_control_scheduling_obligation(
        self,
    ) -> ControlSchedulingObligation | None:
        pending = self._pending_control_scheduling_obligation
        self._clear_pending_control_scheduling_obligation()
        return pending

    def _apply_control_scheduling_decision(
        self,
        decision: GateDecision,
    ) -> None:
        selected = self._select_effective_control_scheduling_obligation(decision)
        if selected is None:
            self._pending_control_scheduling_obligation = None
            self._next_send_ts_ns_local = decision.next_send_ts_ns_local
            return
        if self._pending_control_scheduling_obligation == selected:
            self._next_send_ts_ns_local = selected.due_ts_ns_local
            return
        self._pending_control_scheduling_obligation = selected
        self._next_send_ts_ns_local = selected.due_ts_ns_local

    def _apply_control_scheduling_obligation(
        self,
        obligation: ControlSchedulingObligation | None,
    ) -> None:
        if obligation is None:
            return
        if self._pending_control_scheduling_obligation == obligation:
            self._next_send_ts_ns_local = obligation.due_ts_ns_local
            return
        self._pending_control_scheduling_obligation = obligation
        self._next_send_ts_ns_local = obligation.due_ts_ns_local

    def _dispatch_accepted_intents(
        self,
        accepted_now: list[OrderIntent],
        execution: OrderSubmissionGateway,
        *,
        sim_now_ns: int,
    ) -> list[tuple[OrderIntent, str]]:
        if not accepted_now:
            return []

        execution_errors = execution.apply_intents(accepted_now)
        failed_keys = {
            (it.instrument, it.client_order_id)
            for it, _ in execution_errors
        }

        for it in accepted_now:
            if (it.instrument, it.client_order_id) in failed_keys:
                continue
            if it.intent_type == "new":
                self._process_canonical_order_submitted_event(
                    it,
                    ts_ns_local_dispatch=sim_now_ns,
                )
            self.strategy_state.mark_intent_sent(
                it.instrument,
                it.client_order_id,
                it.intent_type,
            )

        return execution_errors

    def _finalize_decision_effects(
        self,
        *,
        decision: GateDecision,
        execution: OrderSubmissionGateway,
        sim_now_ns: int,
        instrument: str,
    ) -> None:
        execution_errors = self._dispatch_accepted_intents(
            decision.accepted_now,
            execution,
            sim_now_ns=sim_now_ns,
        )

        if execution_errors:
            for it, reason in execution_errors:
                decision.execution_rejected.append(
                    RejectedIntent(it, reason)
                )

        self.strategy.on_risk_decision(decision)
        self._apply_control_scheduling_decision(decision)

        # If there are queued intents but the gate did not provide a next_send_ts_ns_local,
        # wake up at the next second boundary to ensure progress.
        if self._next_send_ts_ns_local is None:
            queue = self.strategy_state.queued_intents.setdefault(
                instrument,
                deque(),
            )
            if queue:
                sec = sim_now_ns // 1_000_000_000
                self._next_send_ts_ns_local = (sec + 1) * 1_000_000_000

    def run(
        self,
        venue: VenueAdapter,
        execution: OrderSubmissionGateway,
        recorder: Any,
    ) -> None:
        """Run the backtest loop."""
        # pylint: disable=too-many-locals,too-many-branches,too-many-statements

        instrument = self.engine_cfg.instrument
        # Initialize hftbacktest engine
        # Fetch very first event block to set local timestamp
        venue.wait_next(timeout_ns=MAX_TIMEOUT_NS, include_order_resp=False)
        observed_local_ns = venue.current_timestamp_ns()
        self.strategy_state.update_timestamp(observed_local_ns)
        sim_now_ns = self.strategy_state.sim_ts_ns_local

        while True:
            timeout_ns = self._compute_timeout_ns(self.strategy_state.sim_ts_ns_local)
            rc = venue.wait_next(timeout_ns=timeout_ns, include_order_resp=True)

            if rc == 1:
                self._close_event_bus()
                break

            observed_local_ns = venue.current_timestamp_ns()
            self.strategy_state.update_timestamp(observed_local_ns)
            sim_now_ns = self.strategy_state.sim_ts_ns_local

            raw_intents: list[OrderIntent] = []
            market_step_result: CoreStepResult | None = None
            control_step_result: CoreStepResult | None = None
            market_event: MarketEvent | None = None

            # -----------------------------------------------------------------
            # Market update
            # -----------------------------------------------------------------
            if rc == 2:
                depth = venue.read_market_snapshot()

                bids: list[BookLevel] = []
                asks: list[BookLevel] = []

                max_levels = max(0, int(self.engine_cfg.max_price_tick_levels))
                if max_levels > 0:
                    roi_lb_tick = depth.roi_lb_tick
                    tick_size = depth.tick_size

                    # -----------------------
                    # ASK side (fixed ticks)
                    # -----------------------
                    for offset in range(max_levels):
                        price_tick = depth.best_ask_tick + offset
                        i = price_tick - roi_lb_tick

                        qty = 0.0
                        if 0 <= i < len(depth.ask_depth):
                            qty = depth.ask_depth[i]

                        asks.append(
                            BookLevel(
                                price=Price(
                                    currency="UNKNOWN",
                                    value=price_tick * tick_size,
                                ),
                                quantity=Quantity(
                                    value=qty,
                                    unit="contracts",
                                ),
                            )
                        )

                    # -----------------------
                    # BID side (fixed ticks)
                    # -----------------------
                    for offset in range(max_levels):
                        price_tick = depth.best_bid_tick - offset
                        i = price_tick - roi_lb_tick

                        qty = 0.0
                        if 0 <= i < len(depth.bid_depth):
                            qty = depth.bid_depth[i]

                        bids.append(
                            BookLevel(
                                price=Price(
                                    currency="UNKNOWN",
                                    value=price_tick * tick_size,
                                ),
                                quantity=Quantity(
                                    value=qty,
                                    unit="contracts",
                                ),
                            )
                        )

                market_event = MarketEvent(
                    ts_ns_exch=sim_now_ns,
                    ts_ns_local=sim_now_ns,
                    instrument=instrument,
                    event_type="book",
                    book=BookPayload(
                        book_type="snapshot",
                        bids=bids,
                        asks=asks,
                        depth=min(len(bids), len(asks)),
                    ),
                )

                constraints = self.risk.build_constraints(sim_now_ns)
                if getattr(self, "_enable_core_step_wakeup_collapse", False):
                    pass
                else:
                    market_step_result = self._process_canonical_market_event(
                        market_event,
                        constraints=constraints,
                    )
                    if not getattr(self, "_enable_core_step_market_dispatch", False):
                        raw_intents.extend(market_step_result.generated_intents)

            # -----------------------------------------------------------------
            # Order / account update
            # -----------------------------------------------------------------
            if rc == 3:
                state_values, orders = venue.read_orders_snapshot()

                self.strategy_state.update_account(
                    instrument=instrument,
                    position=state_values.position,
                    balance=state_values.balance,
                    fee=state_values.fee,
                    trading_volume=state_values.trading_volume,
                    trading_value=state_values.trading_value,
                    num_trades=state_values.num_trades,
                )
                self.strategy_state.ingest_order_snapshots(
                    instrument,
                    orders.values(),
                )

                constraints = self.risk.build_constraints(sim_now_ns)
                raw_intents.extend(
                    self.strategy.on_order_update(
                        self.strategy_state,
                        self.engine_cfg,
                        constraints,
                    )
                )

            # -----------------------------------------------------------------
            # Queue flush
            # -----------------------------------------------------------------
            scheduled_deadline_ns: int | None = None
            scheduling_obligation: ControlSchedulingObligation | None = None
            if self._pending_control_scheduling_obligation is not None:
                scheduling_obligation = self._pending_control_scheduling_obligation
                scheduled_deadline_ns = scheduling_obligation.due_ts_ns_local
            elif self._next_send_ts_ns_local is not None:
                # Transitional compatibility for scalar-only decisions.
                scheduled_deadline_ns = self._next_send_ts_ns_local
            if (
                scheduled_deadline_ns is not None
                and sim_now_ns >= scheduled_deadline_ns
            ):
                if (
                    scheduled_deadline_ns
                    != self._last_injected_control_deadline_ns
                ):
                    if getattr(self, "_enable_core_step_wakeup_collapse", False):
                        pass
                    else:
                        control_step_result = self._process_canonical_control_time_event(
                            instrument=instrument,
                            now_ts_ns_local=sim_now_ns,
                            sim_now_ns=sim_now_ns,
                            scheduled_deadline_ns=scheduled_deadline_ns,
                            scheduling_obligation=scheduling_obligation,
                        )
                        self._last_injected_control_deadline_ns = scheduled_deadline_ns
                        if scheduling_obligation is not None:
                            self._consume_pending_control_scheduling_obligation()
                        else:
                            self._next_send_ts_ns_local = None

            if getattr(self, "_enable_core_step_wakeup_collapse", False):
                collapse_events: list[object] = []
                included_control_time = False
                if market_event is not None:
                    collapse_events.append(market_event)
                if (
                    scheduled_deadline_ns is not None
                    and sim_now_ns >= scheduled_deadline_ns
                    and scheduled_deadline_ns != self._last_injected_control_deadline_ns
                ):
                    collapse_events.append(
                        self._build_control_time_event(
                            sim_now_ns=sim_now_ns,
                            scheduled_deadline_ns=scheduled_deadline_ns,
                            scheduling_obligation=scheduling_obligation,
                        )
                    )
                    included_control_time = True

                if collapse_events:
                    wakeup_entries = self._allocate_wakeup_entries(collapse_events)
                    collapse_constraints = self.risk.build_constraints(sim_now_ns)
                    strategy_evaluator = None
                    if market_event is not None:
                        strategy_evaluator = _LegacyOnFeedStrategyEvaluator(
                            strategy=self.strategy,
                            engine_cfg=self.engine_cfg,
                            constraints=collapse_constraints,
                        )
                    (
                        policy_admission_context,
                        execution_control_apply_context,
                    ) = self._build_policy_and_apply_context(
                        now_ts_ns_local=sim_now_ns,
                    )
                    wakeup_result = run_core_wakeup_step(
                        self.strategy_state,
                        wakeup_entries,
                        configuration=self._core_cfg,
                        strategy_evaluator=strategy_evaluator,
                        strategy_event_filter=lambda event: isinstance(event, MarketEvent),
                        snapshot_instrument=instrument,
                        policy_admission_context=policy_admission_context,
                        execution_control_apply_context=execution_control_apply_context,
                    )
                    self._commit_wakeup_entries(wakeup_entries)
                    if included_control_time and scheduled_deadline_ns is not None:
                        self._last_injected_control_deadline_ns = scheduled_deadline_ns
                        if scheduling_obligation is not None:
                            self._consume_pending_control_scheduling_obligation()
                        else:
                            self._next_send_ts_ns_local = None
                    self._last_core_step_execution_errors = self._dispatch_accepted_intents(
                        list(wakeup_result.dispatchable_intents),
                        execution,
                        sim_now_ns=sim_now_ns,
                    )
                    if wakeup_result.control_scheduling_obligation is None:
                        self._clear_pending_control_scheduling_obligation()
                    else:
                        self._apply_control_scheduling_obligation(
                            wakeup_result.control_scheduling_obligation
                        )
            elif control_step_result is not None:
                if getattr(self, "_enable_core_step_control_time_dispatch", False):
                    self._last_core_step_execution_errors = (
                        self._dispatch_accepted_intents(
                            list(control_step_result.dispatchable_intents),
                            execution,
                            sim_now_ns=sim_now_ns,
                        )
                    )
                    if control_step_result.control_scheduling_obligation is None:
                        self._clear_pending_control_scheduling_obligation()
                    else:
                        self._apply_control_scheduling_obligation(
                            control_step_result.control_scheduling_obligation
                        )
                else:
                    if control_step_result.compat_gate_decision is not None:
                        self._finalize_decision_effects(
                            decision=control_step_result.compat_gate_decision,
                            execution=execution,
                            sim_now_ns=sim_now_ns,
                            instrument=instrument,
                        )
                    elif control_step_result.dispatchable_intents:
                        self._dispatch_accepted_intents(
                            list(control_step_result.dispatchable_intents),
                            execution,
                            sim_now_ns=sim_now_ns,
                        )
                    elif control_step_result.control_scheduling_obligation is not None:
                        self._apply_control_scheduling_obligation(
                            control_step_result.control_scheduling_obligation
                        )

            if (
                not getattr(self, "_enable_core_step_wakeup_collapse", False)
                and
                market_step_result is not None
                and getattr(self, "_enable_core_step_market_dispatch", False)
            ):
                self._last_core_step_execution_errors = self._dispatch_accepted_intents(
                    list(market_step_result.dispatchable_intents),
                    execution,
                    sim_now_ns=sim_now_ns,
                )
                if market_step_result.control_scheduling_obligation is None:
                    self._clear_pending_control_scheduling_obligation()
                else:
                    self._apply_control_scheduling_obligation(
                        market_step_result.control_scheduling_obligation
                    )

            # -----------------------------------------------------------------
            # Gate + execution
            # -----------------------------------------------------------------
            if raw_intents:
                combined = self._sort_intents_for_gate(raw_intents)

                decision = self.risk.decide_intents(
                    raw_intents=combined,
                    state=self.strategy_state,
                    now_ts_ns_local=sim_now_ns,
                )
                self._finalize_decision_effects(
                    decision=decision,
                    execution=execution,
                    sim_now_ns=sim_now_ns,
                    instrument=instrument,
                )

            venue.record(recorder)
