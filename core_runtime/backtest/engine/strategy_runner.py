"""Strategy execution loop for HFT backtests."""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any

from tradingchassis_core import (
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
    OrderExecutionFeedbackEvent,
    OrderIntent,
    OrderSubmittedEvent,
    Price,
    Quantity,
)
from tradingchassis_core.core.events.event_bus import EventBus
from tradingchassis_core.core.events.sinks.sink_logging import LoggingEventSink
from tradingchassis_core.core.execution_control.execution_control import ExecutionControl
from tradingchassis_core.core.execution_control.types import (
    ControlSchedulingObligation,
)
from tradingchassis_core.core.risk.risk_config import RiskConfig
from tradingchassis_core.core.risk.risk_engine import RiskEngine

from core_runtime.backtest.adapters.protocols import OrderSubmissionGateway, VenueAdapter
from core_runtime.backtest.engine.event_stream_cursor import EventStreamCursor
from core_runtime.backtest.events.sinks.file_recorder import FileRecorderSink
from core_runtime.backtest.strategy_api import Strategy

if TYPE_CHECKING:
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


class _LegacyOnOrderUpdateStrategyEvaluator:
    """Runtime-local adapter from legacy Strategy.on_order_update to CoreStep."""

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
            self._strategy.on_order_update(
                context.state,
                self._engine_cfg,
                self._constraints,
            )
        )


class _LegacyWakeupStrategyEvaluator:
    """Runtime-local adapter for one strategy evaluation per wakeup reduction."""

    def __init__(
        self,
        *,
        strategy: Strategy,
        engine_cfg: HftEngineConfig,
        constraints: object,
        market_event: MarketEvent,
    ) -> None:
        self._strategy = strategy
        self._engine_cfg = engine_cfg
        self._constraints = constraints
        self._market_event = market_event

    def evaluate(self, context: object) -> tuple[OrderIntent, ...]:
        return tuple(
            self._strategy.on_feed(
                context.state,
                self._market_event,
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
        enable_core_step_market_dispatch: bool = True,
        enable_core_step_control_time_dispatch: bool = True,
        enable_core_step_wakeup_collapse: bool = False,
        enable_core_step_order_feedback_dispatch: bool = True,
    ) -> None:
        self.engine_cfg = engine_cfg
        self.strategy = strategy
        self._core_cfg = core_cfg
        self._enable_core_step_market_dispatch = enable_core_step_market_dispatch
        self._enable_core_step_control_time_dispatch = (
            enable_core_step_control_time_dispatch
        )
        self._enable_core_step_wakeup_collapse = enable_core_step_wakeup_collapse
        self._enable_core_step_order_feedback_dispatch = (
            enable_core_step_order_feedback_dispatch
        )
        if not self._enable_core_step_market_dispatch:
            raise ValueError("clean-core runtime requires enable_core_step_market_dispatch=True")
        if not self._enable_core_step_control_time_dispatch:
            raise ValueError(
                "clean-core runtime requires enable_core_step_control_time_dispatch=True"
            )
        if not self._enable_core_step_order_feedback_dispatch:
            raise ValueError(
                "clean-core runtime requires enable_core_step_order_feedback_dispatch=True"
            )
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
        )
        self.execution_control = ExecutionControl()

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
                execution_control=self.execution_control,
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
        (
            policy_admission_context,
            execution_control_apply_context,
        ) = self._build_policy_and_apply_context(
            now_ts_ns_local=market_event.ts_ns_local,
        )
        result = run_core_step(
            self.strategy_state,
            entry,
            configuration=self._core_cfg,
            strategy_evaluator=_LegacyOnFeedStrategyEvaluator(
                strategy=self.strategy,
                engine_cfg=self.engine_cfg,
                constraints=constraints,
            ),
            policy_admission_context=policy_admission_context,
            execution_control_apply_context=execution_control_apply_context,
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

    def _build_order_execution_feedback_event(
        self,
        *,
        instrument: str,
        sim_now_ns: int,
        state_values: object,
    ) -> OrderExecutionFeedbackEvent:
        return OrderExecutionFeedbackEvent(
            ts_ns_local_feedback=sim_now_ns,
            instrument=instrument,
            position=float(state_values.position),
            balance=float(state_values.balance),
            fee=float(state_values.fee),
            trading_volume=float(state_values.trading_volume),
            trading_value=float(state_values.trading_value),
            num_trades=int(state_values.num_trades),
            runtime_correlation=None,
        )

    def _process_canonical_order_feedback_event(
        self,
        order_feedback_event: OrderExecutionFeedbackEvent,
        *,
        constraints: object,
    ) -> CoreStepResult:
        position = self._event_stream_cursor.attempt_position()
        entry = EventStreamEntry(
            position=position,
            event=order_feedback_event,
        )
        (
            policy_admission_context,
            execution_control_apply_context,
        ) = self._build_policy_and_apply_context(
            now_ts_ns_local=order_feedback_event.ts_ns_local_feedback,
        )
        result = run_core_step(
            self.strategy_state,
            entry,
            configuration=self._core_cfg,
            strategy_evaluator=_LegacyOnOrderUpdateStrategyEvaluator(
                strategy=self.strategy,
                engine_cfg=self.engine_cfg,
                constraints=constraints,
            ),
            policy_admission_context=policy_admission_context,
            execution_control_apply_context=execution_control_apply_context,
        )
        self._event_stream_cursor.commit_success(position)
        return result

    def _process_canonical_control_time_event(
        self,
        *,
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
        (
            policy_admission_context,
            execution_control_apply_context,
        ) = self._build_policy_and_apply_context(
            now_ts_ns_local=now_ts_ns_local,
        )
        result = run_core_step(
            self.strategy_state,
            entry,
            configuration=self._core_cfg,
            policy_admission_context=policy_admission_context,
            execution_control_apply_context=execution_control_apply_context,
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

    def _clear_pending_control_scheduling_obligation(self) -> None:
        self._pending_control_scheduling_obligation = None
        self._next_send_ts_ns_local = None

    def _consume_pending_control_scheduling_obligation(
        self,
    ) -> ControlSchedulingObligation | None:
        pending = self._pending_control_scheduling_obligation
        self._clear_pending_control_scheduling_obligation()
        return pending

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
                # Clean-Core runtime treats OrderSubmittedEvent as the successful
                # NEW dispatch acknowledgment boundary. Calling mark_intent_sent
                # here would recreate NEW inflight state immediately after the
                # submitted reducer clears it and can cause non-terminating
                # inflight-only loops near end-of-data.
                continue
            self.strategy_state.mark_intent_sent(
                it.instrument,
                it.client_order_id,
                it.intent_type,
            )

        return execution_errors

    def run(
        self,
        venue: VenueAdapter,
        execution: OrderSubmissionGateway,
        recorder: Any,
    ) -> None:
        """Run the backtest loop."""
        # pylint: disable=too-many-locals,too-many-branches,too-many-statements
        debug_loop = os.getenv("TRADINGCHASSIS_DEBUG_LOOP", "").strip() in {
            "1",
            "true",
            "TRUE",
            "yes",
            "YES",
        }
        debug_max_iterations_raw = os.getenv("TRADINGCHASSIS_DEBUG_MAX_ITERATIONS", "").strip()
        debug_max_iterations: int | None = None
        if debug_max_iterations_raw:
            parsed = int(debug_max_iterations_raw)
            if parsed <= 0:
                raise ValueError("TRADINGCHASSIS_DEBUG_MAX_ITERATIONS must be > 0")
            debug_max_iterations = parsed
        debug_every_raw = os.getenv("TRADINGCHASSIS_DEBUG_LOOP_EVERY", "").strip()
        debug_every = 100
        if debug_every_raw:
            parsed_every = int(debug_every_raw)
            if parsed_every <= 0:
                raise ValueError("TRADINGCHASSIS_DEBUG_LOOP_EVERY must be > 0")
            debug_every = parsed_every
        debug_logger = logging.getLogger("core_runtime.backtest.loop")
        loop_iteration = 0
        last_rc: int | None = None
        last_timeout_ns: int | None = None
        last_sim_ts_before_wait: int | None = None
        last_sim_ts_after_wait: int | None = None
        no_progress_iterations = 0
        recorder_exhausted_count = 0
        end_signal_count = 0
        last_debug_signature: tuple[object, ...] | None = None

        instrument = self.engine_cfg.instrument
        # Initialize hftbacktest engine
        # Fetch very first event block to set local timestamp
        venue.wait_next(timeout_ns=MAX_TIMEOUT_NS, include_order_resp=False)
        observed_local_ns = venue.current_timestamp_ns()
        self.strategy_state.update_timestamp(observed_local_ns)
        sim_now_ns = self.strategy_state.sim_ts_ns_local

        while True:
            loop_iteration += 1
            if (
                debug_max_iterations is not None
                and loop_iteration > debug_max_iterations
            ):
                pending_due = (
                    None
                    if self._pending_control_scheduling_obligation is None
                    else self._pending_control_scheduling_obligation.due_ts_ns_local
                )
                pending_reason = (
                    None
                    if self._pending_control_scheduling_obligation is None
                    else self._pending_control_scheduling_obligation.reason
                )
                queued_count = sum(
                    len(queue)
                    for queue in self.strategy_state.queued_intents.values()
                )
                inflight_count = sum(
                    len(bucket)
                    for bucket in self.strategy_state.inflight.values()
                )
                raise RuntimeError(
                    "TRADINGCHASSIS_DEBUG_MAX_ITERATIONS exceeded in HftStrategyRunner.run: "
                    f"iteration={loop_iteration}, sim_ts_ns_local={self.strategy_state.sim_ts_ns_local}, "
                    f"pending_due={pending_due}, pending_reason={pending_reason}, "
                    f"last_injected_control_deadline_ns={self._last_injected_control_deadline_ns}, "
                    f"queued_count={queued_count}, inflight_count={inflight_count}, "
                    f"last_rc={last_rc}, last_timeout_ns={last_timeout_ns}, "
                    f"sim_ts_before_wait={last_sim_ts_before_wait}, sim_ts_after_wait={last_sim_ts_after_wait}, "
                    f"no_progress_iterations={no_progress_iterations}, "
                    f"recorder_exhausted_count={recorder_exhausted_count}, end_signal_count={end_signal_count}"
                )

            sim_ts_before_wait = self.strategy_state.sim_ts_ns_local
            timeout_ns = self._compute_timeout_ns(sim_ts_before_wait)
            rc = venue.wait_next(timeout_ns=timeout_ns, include_order_resp=True)
            last_rc = rc
            last_timeout_ns = timeout_ns
            last_sim_ts_before_wait = sim_ts_before_wait

            if rc == 1:
                end_signal_count += 1
                self._close_event_bus()
                break

            observed_local_ns = venue.current_timestamp_ns()
            self.strategy_state.update_timestamp(observed_local_ns)
            sim_now_ns = self.strategy_state.sim_ts_ns_local
            last_sim_ts_after_wait = sim_now_ns
            venue_progressed = sim_now_ns > sim_ts_before_wait

            market_step_result: CoreStepResult | None = None
            control_step_result: CoreStepResult | None = None
            order_feedback_step_result: CoreStepResult | None = None
            market_event: MarketEvent | None = None
            market_processed = False
            order_feedback_processed = False
            control_time_injected = False
            dispatch_attempted_count = 0
            stale_obligation_cleared = False

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
                    market_processed = True

            # -----------------------------------------------------------------
            # Order / account update
            # -----------------------------------------------------------------
            if rc == 3:
                state_values, orders = venue.read_orders_snapshot()
                _ = orders  # runtime keeps raw snapshot ownership; core receives canonical feedback
                constraints = self.risk.build_constraints(sim_now_ns)
                order_feedback_event = self._build_order_execution_feedback_event(
                    instrument=instrument,
                    sim_now_ns=sim_now_ns,
                    state_values=state_values,
                )
                order_feedback_step_result = self._process_canonical_order_feedback_event(
                    order_feedback_event,
                    constraints=constraints,
                )
                order_feedback_processed = True

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
                scheduling_obligation is not None
                and scheduled_deadline_ns is not None
                and sim_now_ns >= scheduled_deadline_ns
                and scheduled_deadline_ns == self._last_injected_control_deadline_ns
            ):
                # The same control deadline has already been realized once.
                # Keeping it pending would force timeout_ns=0 and can spin forever
                # when venue time no longer advances near end-of-data.
                self._consume_pending_control_scheduling_obligation()
                scheduling_obligation = None
                scheduled_deadline_ns = None
                stale_obligation_cleared = True
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
                            now_ts_ns_local=sim_now_ns,
                            sim_now_ns=sim_now_ns,
                            scheduled_deadline_ns=scheduled_deadline_ns,
                            scheduling_obligation=scheduling_obligation,
                        )
                        control_time_injected = True
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
                        strategy_evaluator = _LegacyWakeupStrategyEvaluator(
                            strategy=self.strategy,
                            engine_cfg=self.engine_cfg,
                            constraints=collapse_constraints,
                            market_event=market_event,
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
                        wakeup_strategy_evaluator=strategy_evaluator,
                        queued_instrument=instrument,
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
                    dispatch_attempted_count += len(wakeup_result.dispatchable_intents)
                    if wakeup_result.control_scheduling_obligation is None:
                        self._clear_pending_control_scheduling_obligation()
                    else:
                        self._apply_control_scheduling_obligation(
                            wakeup_result.control_scheduling_obligation
                        )
            elif control_step_result is not None:
                self._last_core_step_execution_errors = (
                    self._dispatch_accepted_intents(
                        list(control_step_result.dispatchable_intents),
                        execution,
                        sim_now_ns=sim_now_ns,
                    )
                )
                dispatch_attempted_count += len(control_step_result.dispatchable_intents)
                if control_step_result.control_scheduling_obligation is None:
                    self._clear_pending_control_scheduling_obligation()
                else:
                    self._apply_control_scheduling_obligation(
                        control_step_result.control_scheduling_obligation
                    )

            if (
                not getattr(self, "_enable_core_step_wakeup_collapse", False)
                and market_step_result is not None
            ):
                self._last_core_step_execution_errors = self._dispatch_accepted_intents(
                    list(market_step_result.dispatchable_intents),
                    execution,
                    sim_now_ns=sim_now_ns,
                )
                dispatch_attempted_count += len(market_step_result.dispatchable_intents)
                if market_step_result.control_scheduling_obligation is None:
                    self._clear_pending_control_scheduling_obligation()
                else:
                    self._apply_control_scheduling_obligation(
                        market_step_result.control_scheduling_obligation
                    )

            if order_feedback_step_result is not None:
                self._last_core_step_execution_errors = self._dispatch_accepted_intents(
                    list(order_feedback_step_result.dispatchable_intents),
                    execution,
                    sim_now_ns=sim_now_ns,
                )
                dispatch_attempted_count += len(order_feedback_step_result.dispatchable_intents)
                if order_feedback_step_result.control_scheduling_obligation is None:
                    self._clear_pending_control_scheduling_obligation()
                else:
                    self._apply_control_scheduling_obligation(
                        order_feedback_step_result.control_scheduling_obligation
                    )

            pending_due = (
                None
                if self._pending_control_scheduling_obligation is None
                else self._pending_control_scheduling_obligation.due_ts_ns_local
            )
            pending_reason = (
                None
                if self._pending_control_scheduling_obligation is None
                else self._pending_control_scheduling_obligation.reason
            )
            dispatchable_market = (
                0
                if market_step_result is None
                else len(market_step_result.dispatchable_intents)
            )
            dispatchable_control = (
                0
                if control_step_result is None
                else len(control_step_result.dispatchable_intents)
            )
            dispatchable_feedback = (
                0
                if order_feedback_step_result is None
                else len(order_feedback_step_result.dispatchable_intents)
            )
            queued_count = sum(
                len(queue)
                for queue in self.strategy_state.queued_intents.values()
            )
            inflight_count = sum(
                len(bucket)
                for bucket in self.strategy_state.inflight.values()
            )
            queued_keys = tuple(
                f"{instrument_key}:{queued.logical_key}"
                for instrument_key in sorted(self.strategy_state.queued_intents)
                for queued in list(self.strategy_state.queued_intents[instrument_key])[:3]
            )[:6]
            inflight_keys = tuple(
                f"{instrument_key}:{client_order_id}"
                for instrument_key in sorted(self.strategy_state.inflight)
                for client_order_id in sorted(self.strategy_state.inflight[instrument_key])[:3]
            )[:6]
            event_processed = market_processed or order_feedback_processed or control_time_injected
            has_pending_core_work = (
                queued_count > 0
                or inflight_count > 0
                or self._pending_control_scheduling_obligation is not None
                or self._next_send_ts_ns_local is not None
            )
            no_event_rc = rc not in {1, 2, 3}
            no_progress_iteration = (
                not venue_progressed
                and not event_processed
                and dispatch_attempted_count == 0
                and no_event_rc
            )
            if no_progress_iteration:
                no_progress_iterations += 1
            else:
                no_progress_iterations = 0
            termination_no_work_no_progress = (
                not has_pending_core_work
                and no_progress_iteration
            )

            if debug_loop:
                debug_signature = (
                    rc,
                    timeout_ns,
                    sim_ts_before_wait,
                    sim_now_ns,
                    has_pending_core_work,
                    queued_count,
                    inflight_count,
                    pending_due,
                    pending_reason,
                    market_processed,
                    order_feedback_processed,
                    control_time_injected,
                    dispatch_attempted_count,
                    termination_no_work_no_progress,
                )
                should_emit_debug = (
                    loop_iteration == 1
                    or (loop_iteration % debug_every) == 0
                    or debug_signature != last_debug_signature
                    or termination_no_work_no_progress
                )
                if should_emit_debug:
                    debug_logger.info(
                    "loop=%s rc=%s sim_ts_ns_local=%s timeout_ns=%s market_processed=%s "
                    "order_feedback_processed=%s control_time_injected=%s "
                    "pending_due=%s pending_reason=%s stale_obligation_cleared=%s "
                    "dispatchable_market=%s dispatchable_control=%s dispatchable_feedback=%s "
                    "dispatch_attempted=%s queued_count=%s inflight_count=%s "
                    "queued_keys=%s inflight_keys=%s sim_ts_before_wait=%s "
                    "sim_ts_after_wait=%s no_progress_iterations=%s has_pending_core_work=%s "
                    "termination_no_work_no_progress=%s no_event_rc=%s",
                        loop_iteration,
                        rc,
                        sim_now_ns,
                        timeout_ns,
                        market_processed,
                        order_feedback_processed,
                        control_time_injected,
                        pending_due,
                        pending_reason,
                        stale_obligation_cleared,
                        dispatchable_market,
                        dispatchable_control,
                        dispatchable_feedback,
                        dispatch_attempted_count,
                        queued_count,
                        inflight_count,
                        queued_keys,
                        inflight_keys,
                        sim_ts_before_wait,
                        sim_now_ns,
                        no_progress_iterations,
                        has_pending_core_work,
                        termination_no_work_no_progress,
                        no_event_rc,
                    )
                    print(
                        "[TRADINGCHASSIS_DEBUG_LOOP] "
                        f"loop={loop_iteration} rc={rc} timeout_ns={timeout_ns} "
                        f"sim_ts_before_wait={sim_ts_before_wait} sim_ts_after_wait={sim_now_ns} "
                        f"market_processed={market_processed} order_feedback_processed={order_feedback_processed} "
                        f"control_time_injected={control_time_injected} dispatch_attempted={dispatch_attempted_count} "
                        f"pending_due={pending_due} pending_reason={pending_reason} "
                        f"queued_count={queued_count} inflight_count={inflight_count} "
                        f"queued_keys={queued_keys} inflight_keys={inflight_keys} "
                        f"no_progress_iterations={no_progress_iterations} "
                        f"termination_no_work_no_progress={termination_no_work_no_progress} "
                        f"no_event_rc={no_event_rc}",
                        flush=True,
                    )
                last_debug_signature = debug_signature

            if termination_no_work_no_progress:
                if debug_loop:
                    print(
                        "[TRADINGCHASSIS_DEBUG_LOOP] breaking no-work/no-progress loop",
                        flush=True,
                    )
                self._close_event_bus()
                break

            recorder_exhausted = bool(venue.record(recorder))
            if recorder_exhausted:
                recorder_exhausted_count += 1
