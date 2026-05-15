from __future__ import annotations

from dataclasses import dataclass

from tradingchassis_core.core.domain.types import (
    BookLevel,
    BookPayload,
    MarketEvent,
    Price,
    Quantity,
    RiskConstraints,
)

from core_runtime.strategies.debug_strategy import DebugStrategyV1


@dataclass
class _StateStub:
    busy: bool = False

    def has_working_order(self, instrument: str, client_order_id: str) -> bool:
        _ = (instrument, client_order_id)
        return self.busy

    def has_inflight(self, instrument: str, client_order_id: str) -> bool:
        _ = (instrument, client_order_id)
        return self.busy

    def has_queued_intent(self, instrument: str, client_order_id: str) -> bool:
        _ = (instrument, client_order_id)
        return self.busy


@dataclass(frozen=True)
class _EngineCfgStub:
    tick_size: float = 0.1


def test_debug_strategy_uses_clean_state_busy_checks_without_legacy_method() -> None:
    strategy = DebugStrategyV1(
        spread=5.0,
        order_qty=0.1,
        use_price_tick_levels=1,
        post_only=True,
    )
    state = _StateStub(busy=False)
    event = MarketEvent(
        ts_ns_exch=1,
        ts_ns_local=1,
        instrument="BTC_USDC-PERPETUAL",
        event_type="book",
        book=BookPayload(
            book_type="snapshot",
            bids=(
                BookLevel(
                    price=Price(currency="UNKNOWN", value=100.0),
                    quantity=Quantity(value=1.0, unit="contracts"),
                ),
            ),
            asks=(
                BookLevel(
                    price=Price(currency="UNKNOWN", value=101.0),
                    quantity=Quantity(value=1.0, unit="contracts"),
                ),
            ),
            depth=1,
        ),
    )
    constraints = RiskConstraints(
        ts_ns_local=1,
        scope="test",
        trading_enabled=True,
    )

    intents = strategy.on_feed(
        state=state,  # type: ignore[arg-type]
        event=event,
        engine_cfg=_EngineCfgStub(),
        constraints=constraints,
    )

    assert len(intents) == 2
    assert all(intent.intent_type == "new" for intent in intents)
