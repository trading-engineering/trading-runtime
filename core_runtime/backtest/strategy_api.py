"""Runtime-local Strategy protocol and config model.

Core no longer exports strategy construction interfaces; runtime owns these.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Protocol, runtime_checkable

from tradingchassis_core.core.domain.state import StrategyState
from tradingchassis_core.core.domain.types import MarketEvent, OrderIntent, RiskConstraints


@runtime_checkable
class Strategy(Protocol):
    """Runtime strategy callback contract."""

    def on_feed(
        self,
        state: StrategyState,
        event: MarketEvent,
        engine_cfg: object,
        constraints: RiskConstraints,
    ) -> list[OrderIntent]:
        """Return intents generated for one market event."""

    def on_order_update(
        self,
        state: StrategyState,
        engine_cfg: object,
        constraints: RiskConstraints,
    ) -> list[OrderIntent]:
        """Return intents generated for one execution-feedback update."""


@dataclass(frozen=True, slots=True)
class StrategyConfig:
    """Runtime-local strategy constructor config."""

    class_path: str
    params: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any]) -> StrategyConfig:
        if "class_path" not in raw:
            raise ValueError("strategy.class_path is required")
        class_path = str(raw["class_path"])
        params = {
            key: value
            for key, value in raw.items()
            if key != "class_path"
        }
        return cls(class_path=class_path, params=params)

    def to_engine_params(self) -> dict[str, Any]:
        return dict(self.params)
