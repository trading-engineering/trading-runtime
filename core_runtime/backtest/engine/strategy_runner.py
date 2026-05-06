"""Strategy execution loop for HFT backtests."""

from __future__ import annotations

import logging
from collections import deque
from pathlib import Path
from typing import TYPE_CHECKING, Any

from tradingchassis_core import run_core_step
from tradingchassis_core.core.domain.configuration import CoreConfiguration
from tradingchassis_core.core.domain.processing import process_event_entry
from tradingchassis_core.core.domain.processing_order import (
    EventStreamEntry,
)
from tradingchassis_core.core.domain.state import StrategyState
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
    ) -> None:
        self.engine_cfg = engine_cfg
        self.strategy = strategy
        self._core_cfg = core_cfg

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

    def _process_canonical_market_event(self, market_event: MarketEvent) -> None:
        self._process_canonical_event(market_event)

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
        sim_now_ns: int,
        scheduled_deadline_ns: int,
        scheduling_obligation: ControlSchedulingObligation | None = None,
    ) -> None:
        obligation_reason = "rate_limit"
        obligation_due_ts_ns_local = scheduled_deadline_ns
        if scheduling_obligation is not None:
            obligation_reason = scheduling_obligation.reason
            obligation_due_ts_ns_local = scheduling_obligation.due_ts_ns_local
        control_time_event = ControlTimeEvent(
            ts_ns_local_control=sim_now_ns,
            reason="scheduled_control_recheck",
            due_ts_ns_local=scheduled_deadline_ns,
            realized_ts_ns_local=sim_now_ns,
            obligation_reason=obligation_reason,
            obligation_due_ts_ns_local=obligation_due_ts_ns_local,
            runtime_correlation=None,
        )
        position = self._event_stream_cursor.attempt_position()
        entry = EventStreamEntry(
            position=position,
            event=control_time_event,
        )
        _ = run_core_step(
            self.strategy_state,
            entry,
            configuration=self._core_cfg,
        )
        self._event_stream_cursor.commit_success(position)

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

                self._process_canonical_market_event(market_event)

                constraints = self.risk.build_constraints(sim_now_ns)
                raw_intents.extend(
                    self.strategy.on_feed(
                        self.strategy_state,
                        market_event,
                        self.engine_cfg,
                        constraints,
                    )
                )

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
                    self._process_canonical_control_time_event(
                        sim_now_ns=sim_now_ns,
                        scheduled_deadline_ns=scheduled_deadline_ns,
                        scheduling_obligation=scheduling_obligation,
                    )
                    self._last_injected_control_deadline_ns = scheduled_deadline_ns
                    if scheduling_obligation is not None:
                        self._consume_pending_control_scheduling_obligation()
                    else:
                        self._next_send_ts_ns_local = None
                    raw_intents.extend(
                        self.strategy_state.pop_queued_intents(instrument)
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

                execution_errors: list[tuple[OrderIntent, str]] = []
                if decision.accepted_now:
                    execution_errors = execution.apply_intents(
                        decision.accepted_now
                    )

                    failed_keys = {
                        (it.instrument, it.client_order_id)
                        for it, _ in execution_errors
                    }

                    for it in decision.accepted_now:
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

            venue.record(recorder)
