"""Phase 4K probe: hftbacktest execution feedback feasibility surface."""

from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import Any

import pytest

from core_runtime.backtest.adapters.execution import (
    HftBacktestExecutionAdapter,
    _to_i64_order_id,
)
from core_runtime.backtest.adapters.venue import HftBacktestVenueAdapter
from core_runtime.backtest.engine.strategy_runner import HftStrategyRunner

hftbacktest = pytest.importorskip("hftbacktest")
from hftbacktest import types as hbt_types  # type: ignore  # noqa: E402
from hftbacktest.binding import ROIVectorMarketDepthBacktest  # type: ignore  # noqa: E402


@dataclass(frozen=True)
class ProbeRow:
    source: str
    explicit_feedback_boundary: bool
    authoritative_filled_price: bool
    authoritative_cum_filled_qty: bool
    authoritative_liquidity_flag: bool
    deterministic_client_order_id_correlation: bool
    deterministic_source_sequence: bool
    batching_safe: bool
    eligible_for_execution_feedback_record_source: bool


def _public_methods(cls: type[Any]) -> set[str]:
    return {
        name
        for name, member in inspect.getmembers(cls)
        if callable(member) and not name.startswith("_")
    }


def test_probe_wrapper_surface_is_snapshot_only_for_rc3_branch() -> None:
    venue_methods = _public_methods(HftBacktestVenueAdapter)
    execution_methods = _public_methods(HftBacktestExecutionAdapter)
    runner_source = inspect.getsource(HftStrategyRunner.run)

    assert "read_orders_snapshot" in venue_methods
    assert "wait_next" in venue_methods
    assert "apply_intents" in execution_methods

    # Probe fact: no adapter-facing execution feedback source is currently exposed.
    assert "wait_order_response" not in venue_methods
    assert "drain_execution_feedback_records" not in venue_methods
    assert "drain_execution_feedback_records" not in execution_methods

    # Probe fact: strategy runner rc==3 branch is snapshot materialization.
    assert "if rc == 3" in runner_source
    assert "read_orders_snapshot()" in runner_source
    assert "state_values, orders = venue.read_orders_snapshot()" in runner_source
    assert "wait_order_response" not in runner_source


def test_probe_hftbacktest_binding_response_surface() -> None:
    _ = hftbacktest
    binding_methods = _public_methods(ROIVectorMarketDepthBacktest)

    assert "wait_next_feed" in binding_methods
    assert "wait_order_response" in binding_methods
    assert "orders" in binding_methods
    assert "state_values" in binding_methods
    assert "last_trades" in binding_methods

    # Probe fact: there is no direct structured feedback drain API.
    assert "drain_execution_feedback_records" not in binding_methods
    assert "execution_feedback_records" not in binding_methods


def test_probe_order_dtype_diagnostics_and_contract_gaps() -> None:
    field_names = set(hbt_types.order_dtype.names or ())

    # Snapshot/order fields currently visible through `orders()`.
    assert {"order_id", "exec_price_tick", "exec_qty", "status", "maker"} <= field_names

    # Required for the conceptual source contract but not present on raw order dtype.
    assert "client_order_id" not in field_names
    assert "source_sequence" not in field_names
    assert "liquidity_flag" not in field_names


def test_probe_wait_order_response_is_status_code_only_and_timeout_ambiguous() -> None:
    source = inspect.getsource(ROIVectorMarketDepthBacktest.wait_order_response)

    assert "def wait_order_response" in source
    assert "-> int64" in source
    assert "reaches the timeout" in source
    assert "Returns:" in source
    assert "order response" in source

    # Probe fact: no structured payload object is returned from this method.
    assert "dict" not in source
    assert "payload" not in source
    assert "record" not in source


def test_probe_wait_next_feed_response_signal_is_any_order_response_only() -> None:
    source = inspect.getsource(ROIVectorMarketDepthBacktest.wait_next_feed)

    assert "include_order_resp" in source
    assert "`3` when it receives an order response" in source
    assert "any order response" in source
    assert "source_sequence" not in source
    assert "order_id" not in source


def test_probe_immediate_order_lookup_fields_and_missing_boundary_fields() -> None:
    field_names = set(hbt_types.order_dtype.names or ())

    # Immediate lookup provides current order state fields.
    assert {
        "order_id",
        "status",
        "req",
        "exec_qty",
        "exec_price_tick",
        "maker",
        "local_timestamp",
        "exch_timestamp",
        "leaves_qty",
    } <= field_names

    # Missing record-boundary/correlation fields for source contract.
    assert "client_order_id" not in field_names
    assert "source_sequence" not in field_names
    assert "explicit_update_kind" not in field_names
    assert "response_sequence" not in field_names
    assert "cum_filled_qty" not in field_names


def test_probe_client_order_id_correlation_is_one_way_without_reverse_map() -> None:
    # Deterministic forward mapping exists.
    assert _to_i64_order_id("cid-123") == _to_i64_order_id("cid-123")
    assert _to_i64_order_id("42") == 42

    adapter_fields = set(HftBacktestExecutionAdapter.__dataclass_fields__.keys())
    apply_intents_source = inspect.getsource(HftBacktestExecutionAdapter.apply_intents)
    id_mapping_source = inspect.getsource(_to_i64_order_id)

    # Probe fact: adapter persists no reverse order_id -> client_order_id correlation map.
    assert adapter_fields == {"hbt", "asset_no"}
    assert "blake2b" in id_mapping_source
    assert "_to_i64_order_id(intent.client_order_id)" in apply_intents_source
    assert "reverse" not in apply_intents_source
    assert "mapping" not in apply_intents_source


def test_probe_wait_order_response_plus_immediate_lookup_candidate_stays_ineligible() -> None:
    row = ProbeRow(
        source="L: wait_order_response + immediate orders().get(order_id)",
        explicit_feedback_boundary=False,
        authoritative_filled_price=False,
        authoritative_cum_filled_qty=False,
        authoritative_liquidity_flag=False,
        deterministic_client_order_id_correlation=False,
        deterministic_source_sequence=False,
        batching_safe=False,
        eligible_for_execution_feedback_record_source=False,
    )

    assert row.eligible_for_execution_feedback_record_source is False
    assert row.explicit_feedback_boundary is False
    assert row.deterministic_source_sequence is False
    assert row.deterministic_client_order_id_correlation is False


def test_probe_contract_matrix_for_candidates_a_b_c() -> None:
    matrix = [
        ProbeRow(
            source="A: direct structured order-response channel",
            explicit_feedback_boundary=False,
            authoritative_filled_price=False,
            authoritative_cum_filled_qty=False,
            authoritative_liquidity_flag=False,
            deterministic_client_order_id_correlation=False,
            deterministic_source_sequence=False,
            batching_safe=False,
            eligible_for_execution_feedback_record_source=False,
        ),
        ProbeRow(
            source="B: rc==3 wakeup + immediate structured lookup",
            explicit_feedback_boundary=False,
            authoritative_filled_price=False,
            authoritative_cum_filled_qty=False,
            authoritative_liquidity_flag=False,
            deterministic_client_order_id_correlation=False,
            deterministic_source_sequence=False,
            batching_safe=False,
            eligible_for_execution_feedback_record_source=False,
        ),
        ProbeRow(
            source="C: snapshot deltas (diagnostic only)",
            explicit_feedback_boundary=False,
            authoritative_filled_price=False,
            authoritative_cum_filled_qty=False,
            authoritative_liquidity_flag=False,
            deterministic_client_order_id_correlation=False,
            deterministic_source_sequence=False,
            batching_safe=False,
            eligible_for_execution_feedback_record_source=False,
        ),
    ]

    assert all(not row.eligible_for_execution_feedback_record_source for row in matrix)
    assert all(not row.explicit_feedback_boundary for row in matrix)
