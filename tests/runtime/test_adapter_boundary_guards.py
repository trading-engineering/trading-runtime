"""Static boundary guard tests for adapter/core layering.

These checks intentionally use lightweight text scanning to catch boundary
drift early without introducing new tooling dependencies.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

CORE_RUNTIME_ROOT = Path(__file__).resolve().parents[2]
WORKSPACE_ROOT = CORE_RUNTIME_ROOT.parent

ADAPTER_FILES = [
    CORE_RUNTIME_ROOT / "trading_runtime/backtest/adapters/venue.py",
    CORE_RUNTIME_ROOT / "trading_runtime/backtest/adapters/execution.py",
    CORE_RUNTIME_ROOT / "trading_runtime/backtest/adapters/protocols.py",
]


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _assert_no_matches(path: Path, patterns: list[str], description: str) -> None:
    content = _read_text(path)
    for pattern in patterns:
        match = re.search(pattern, content, flags=re.MULTILINE)
        assert match is None, f"{path} violates {description}: matched /{pattern}/"


def test_adapters_do_not_import_or_reference_strategy_state() -> None:
    patterns = [
        r"\bStrategyState\b",
    ]
    for path in ADAPTER_FILES:
        _assert_no_matches(path, patterns, "StrategyState boundary")


def test_adapters_do_not_import_or_call_canonical_processing_boundaries() -> None:
    patterns = [
        r"\bprocess_event_entry\b",
        r"\bprocess_canonical_event\b",
        r"\bfold_event_stream_entries\b",
    ]
    for path in ADAPTER_FILES:
        _assert_no_matches(path, patterns, "canonical processing boundary")


def test_adapters_do_not_import_or_construct_fill_event() -> None:
    patterns = [
        r"^\s*from\s+[^\n]*\s+import\s+[^\n]*\bFillEvent\b",
        r"^\s*import\s+[^\n]*\bFillEvent\b",
        r"\bFillEvent\s*\(",
    ]
    for path in ADAPTER_FILES:
        _assert_no_matches(path, patterns, "FillEvent ingress boundary")


def test_core_production_package_does_not_import_trading_runtime() -> None:
    core_pkg = WORKSPACE_ROOT / "core/trading_framework"
    if not core_pkg.exists():
        pytest.skip(
            f"core package not present in this checkout layout: {core_pkg}"
        )

    patterns = [
        r"^\s*import\s+trading_runtime(\.|$)",
        r"^\s*from\s+trading_runtime(\.|\s+import\b)",
    ]

    for path in core_pkg.rglob("*.py"):
        _assert_no_matches(path, patterns, "core->core-runtime import boundary")


def test_protocols_does_not_define_execution_feedback_record_source_yet() -> None:
    protocols_py = CORE_RUNTIME_ROOT / "trading_runtime/backtest/adapters/protocols.py"
    patterns = [
        r"^\s*class\s+ExecutionFeedbackRecordSource\b",
        r"^\s*ExecutionFeedbackRecordSource\s*=",
    ]
    _assert_no_matches(
        protocols_py,
        patterns,
        "deferred ExecutionFeedbackRecordSource capability",
    )
