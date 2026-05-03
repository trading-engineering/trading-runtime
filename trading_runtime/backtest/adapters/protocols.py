"""Typing-only adapter capability protocols for backtest venue sources.

This module introduces low-risk capability seams as ``typing.Protocol`` classes.
It is intentionally implementation-light:

- no runtime behavior or orchestration changes;
- no runtime ``isinstance`` checks;
- no required explicit inheritance for concrete adapters;
- structural compatibility is sufficient.

Current ``HftBacktestVenueAdapter`` already structurally conforms to all
protocols defined here.

Notes on intentionally deferred capabilities:

- ``OrderSubmissionGateway`` is excluded in this slice until additional
  characterization covers concrete hftbacktest execution adapter behavior.
- ``ExecutionFeedbackRecordSource`` is excluded in this slice because
  execution-feedback capability remains deferred and gated by existing
  runtime/source contracts.
"""

from __future__ import annotations

from typing import Any, Protocol


class VenueEventWaiter(Protocol):
    """Wakeup capability for runtime loop progression.

    This is a typing seam only. It does not alter wait semantics, call order,
    timeout computation, or rc-branch interpretation.
    """

    def wait_next(self, *, timeout_ns: int, include_order_resp: bool) -> int:
        """Block until next wakeup and return venue-defined rc code."""


class VenueClock(Protocol):
    """Timestamp-read capability for runtime adoption."""

    def current_timestamp_ns(self) -> int:
        """Return current venue-local timestamp in nanoseconds."""


class MarketInputSource(Protocol):
    """Market snapshot read capability for canonical market mapping."""

    def read_market_snapshot(self) -> Any:
        """Return venue-specific market snapshot object."""


class OrderSnapshotSource(Protocol):
    """Order snapshot capability for compatibility materialization paths.

    The current compatibility boundary consumes a combined tuple from one call.
    A future split may separate this source surface.
    """

    def read_orders_snapshot(self) -> tuple[Any, Any]:
        """Return (state_values, orders) from current snapshot boundary."""


class AccountSnapshotSource(Protocol):
    """Account snapshot capability (currently shared tuple-return surface).

    This intentionally shares ``read_orders_snapshot`` with
    ``OrderSnapshotSource`` in the current runtime shape.
    """

    def read_orders_snapshot(self) -> tuple[Any, Any]:
        """Return (state_values, orders) from current snapshot boundary."""

