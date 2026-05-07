from __future__ import annotations

import pytest
from tradingchassis_core.core.domain.processing_order import ProcessingPosition

from core_runtime.backtest.engine.event_stream_cursor import EventStreamCursor


def test_event_stream_cursor_starts_at_zero() -> None:
    cursor = EventStreamCursor()
    assert cursor.next_index == 0


def test_attempt_position_does_not_advance_cursor() -> None:
    cursor = EventStreamCursor()
    attempted = cursor.attempt_position()
    assert attempted.index == 0
    assert cursor.next_index == 0


def test_attempt_positions_does_not_advance_cursor_and_returns_batch() -> None:
    cursor = EventStreamCursor(start_index=5)
    attempted = cursor.attempt_positions(3)
    assert tuple(position.index for position in attempted) == (5, 6, 7)
    assert cursor.next_index == 5


def test_attempt_positions_rejects_negative_count() -> None:
    cursor = EventStreamCursor()
    with pytest.raises(ValueError, match="count must be >= 0"):
        cursor.attempt_positions(-1)
    assert cursor.next_index == 0


def test_attempt_positions_commit_success_advances_in_batch_order() -> None:
    cursor = EventStreamCursor()
    attempted = cursor.attempt_positions(2)

    cursor.commit_success(attempted[0])
    cursor.commit_success(attempted[1])

    assert cursor.next_index == 2


def test_commit_success_advances_by_one() -> None:
    cursor = EventStreamCursor()
    attempted = cursor.attempt_position()
    cursor.commit_success(attempted)
    assert cursor.next_index == 1


def test_commit_success_rejects_mismatched_position() -> None:
    cursor = EventStreamCursor()
    with pytest.raises(ValueError, match="Committed position does not match expected next index"):
        cursor.commit_success(ProcessingPosition(index=1))
    assert cursor.next_index == 0


def test_repeated_attempt_commit_produces_sequential_positions() -> None:
    cursor = EventStreamCursor()
    observed: list[int] = []
    for _ in range(3):
        position = cursor.attempt_position()
        observed.append(position.index)
        cursor.commit_success(position)

    assert observed == [0, 1, 2]
    assert cursor.next_index == 3
