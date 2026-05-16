"""Runtime-owned canonical processing position cursor."""

from __future__ import annotations

from tradingchassis_core.core.domain.processing_order import ProcessingPosition


class EventStreamCursor:
    """Ordering-only helper for canonical ProcessingPosition allocation."""

    def __init__(self, *, start_index: int = 0) -> None:
        if start_index < 0:
            raise ValueError("start_index must be >= 0")
        self._next_index = start_index

    @property
    def next_index(self) -> int:
        return self._next_index

    def attempt_position(self) -> ProcessingPosition:
        return ProcessingPosition(index=self._next_index)

    def attempt_positions(self, count: int) -> tuple[ProcessingPosition, ...]:
        if count < 0:
            raise ValueError("count must be >= 0")
        return tuple(
            ProcessingPosition(index=self._next_index + offset)
            for offset in range(count)
        )

    def commit_success(self, position: ProcessingPosition) -> None:
        if position.index != self._next_index:
            raise ValueError(
                "Committed position does not match expected next index: "
                f"expected={self._next_index} actual={position.index}"
            )
        self._next_index += 1
