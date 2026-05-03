"""Characterization tests for HftBacktestExecutionAdapter.

These tests lock current adapter submission behavior only. They do not imply:
- FillEvent ingress;
- ExecutionFeedbackRecordSource support;
- canonical execution-feedback authority;
- post-submission lifecycle migration.
"""

from __future__ import annotations

import inspect
from dataclasses import dataclass, field
from typing import Any

from trading_framework.core.domain.reject_reasons import RejectReason
from trading_framework.core.domain.types import (
    CancelOrderIntent,
    NewOrderIntent,
    Price,
    Quantity,
    ReplaceOrderIntent,
)

from trading_runtime.backtest.adapters.execution import (
    HftBacktestExecutionAdapter,
    _to_i64_order_id,
)


@dataclass
class _FakeHbt:
    """Minimal fake hftbacktest object with configurable outcomes."""

    result_codes: dict[str, int] = field(default_factory=dict)
    raise_on: set[str] = field(default_factory=set)
    calls: list[tuple[str, tuple[Any, ...]]] = field(default_factory=list)

    def _maybe_raise(self, method: str) -> None:
        if method in self.raise_on:
            raise RuntimeError(f"{method} failed")

    def _result_code(self, method: str) -> int:
        return self.result_codes.get(method, 0)

    def submit_buy_order(
        self,
        asset_no: int,
        order_id: int,
        price: float,
        quantity: float,
        tif: int,
        order_type: int,
        post_only_flag: bool,
    ) -> int:
        self.calls.append(
            (
                "submit_buy_order",
                (
                    asset_no,
                    order_id,
                    price,
                    quantity,
                    tif,
                    order_type,
                    post_only_flag,
                ),
            )
        )
        self._maybe_raise("submit_buy_order")
        return self._result_code("submit_buy_order")

    def submit_sell_order(
        self,
        asset_no: int,
        order_id: int,
        price: float,
        quantity: float,
        tif: int,
        order_type: int,
        post_only_flag: bool,
    ) -> int:
        self.calls.append(
            (
                "submit_sell_order",
                (
                    asset_no,
                    order_id,
                    price,
                    quantity,
                    tif,
                    order_type,
                    post_only_flag,
                ),
            )
        )
        self._maybe_raise("submit_sell_order")
        return self._result_code("submit_sell_order")

    def modify(
        self,
        asset_no: int,
        order_id: int,
        new_price: float,
        new_quantity: float,
        post_only_flag: bool,
    ) -> int:
        self.calls.append(
            (
                "modify",
                (asset_no, order_id, new_price, new_quantity, post_only_flag),
            )
        )
        self._maybe_raise("modify")
        return self._result_code("modify")

    def cancel(
        self,
        asset_no: int,
        order_id: int,
        post_only_flag: bool,
    ) -> int:
        self.calls.append(("cancel", (asset_no, order_id, post_only_flag)))
        self._maybe_raise("cancel")
        return self._result_code("cancel")


def _new_intent(
    *,
    side: str,
    client_order_id: str,
    order_type: str = "limit",
    tif: str = "GTC",
    intended_price: Price | None = None,
) -> NewOrderIntent:
    return NewOrderIntent(
        ts_ns_local=1,
        instrument="BTC_USDC-PERPETUAL",
        client_order_id=client_order_id,
        intents_correlation_id=f"corr-{client_order_id}",
        side=side,
        order_type=order_type,
        intended_qty=Quantity(value=2.5, unit="contracts"),
        intended_price=(
            intended_price
            if intended_price is not None
            else Price(currency="USDC", value=100.5)
        ),
        time_in_force=tif,
    )


def _replace_intent(*, client_order_id: str = "cid-replace-1") -> ReplaceOrderIntent:
    return ReplaceOrderIntent(
        ts_ns_local=2,
        instrument="BTC_USDC-PERPETUAL",
        client_order_id=client_order_id,
        intents_correlation_id=f"corr-{client_order_id}",
        side="buy",
        order_type="limit",
        intended_qty=Quantity(value=3.0, unit="contracts"),
        intended_price=Price(currency="USDC", value=101.25),
    )


def _cancel_intent(*, client_order_id: str = "cid-cancel-1") -> CancelOrderIntent:
    return CancelOrderIntent(
        ts_ns_local=3,
        instrument="BTC_USDC-PERPETUAL",
        client_order_id=client_order_id,
        intents_correlation_id=f"corr-{client_order_id}",
    )


def test_new_buy_and_sell_submissions_call_expected_hbt_methods_and_map_arguments() -> None:
    fake_hbt = _FakeHbt()
    adapter = HftBacktestExecutionAdapter(hbt=fake_hbt, asset_no=7)

    buy_intent = _new_intent(
        side="buy",
        client_order_id="123",
        tif="IOC",
        order_type="limit",
        intended_price=Price(currency="USDC", value=99.75),
    )
    sell_intent = _new_intent(
        side="sell",
        client_order_id="cid-sell-1",
        tif="POST_ONLY",
        order_type="market",
        intended_price=Price(currency="USDC", value=100.5),
    )

    execution_errors = adapter.apply_intents([buy_intent, sell_intent])

    assert execution_errors == []
    assert len(fake_hbt.calls) == 2

    method_buy, args_buy = fake_hbt.calls[0]
    assert method_buy == "submit_buy_order"
    assert args_buy == (
        7,
        _to_i64_order_id("123"),
        99.75,
        2.5,
        3,  # IOC
        0,  # limit
        False,
    )

    method_sell, args_sell = fake_hbt.calls[1]
    assert method_sell == "submit_sell_order"
    assert args_sell == (
        7,
        _to_i64_order_id("cid-sell-1"),
        100.5,
        2.5,
        1,  # POST_ONLY -> GTX
        1,  # market
        False,
    )


def test_new_submission_nonzero_result_code_returns_exchange_reject() -> None:
    fake_hbt = _FakeHbt(result_codes={"submit_buy_order": 9})
    adapter = HftBacktestExecutionAdapter(hbt=fake_hbt, asset_no=1)
    intent = _new_intent(side="buy", client_order_id="cid-new-reject")

    execution_errors = adapter.apply_intents([intent])

    assert execution_errors == [(intent, RejectReason.EXCHANGE_REJECT)]


def test_new_submission_exception_returns_exchange_error() -> None:
    fake_hbt = _FakeHbt(raise_on={"submit_sell_order"})
    adapter = HftBacktestExecutionAdapter(hbt=fake_hbt, asset_no=1)
    intent = _new_intent(side="sell", client_order_id="cid-new-error")

    execution_errors = adapter.apply_intents([intent])

    assert execution_errors == [(intent, RejectReason.EXCHANGE_ERROR)]


def test_replace_calls_modify_with_expected_mapping_and_success_behavior() -> None:
    fake_hbt = _FakeHbt()
    adapter = HftBacktestExecutionAdapter(hbt=fake_hbt, asset_no=3)
    intent = _replace_intent(client_order_id="cid-replace-ok")

    execution_errors = adapter.apply_intents([intent])

    assert execution_errors == []
    assert fake_hbt.calls == [
        (
            "modify",
            (
                3,
                _to_i64_order_id("cid-replace-ok"),
                101.25,
                3.0,
                False,
            ),
        )
    ]


def test_replace_nonzero_result_code_returns_exchange_reject() -> None:
    fake_hbt = _FakeHbt(result_codes={"modify": 4})
    adapter = HftBacktestExecutionAdapter(hbt=fake_hbt, asset_no=0)
    intent = _replace_intent(client_order_id="cid-replace-reject")

    execution_errors = adapter.apply_intents([intent])

    assert execution_errors == [(intent, RejectReason.EXCHANGE_REJECT)]


def test_replace_exception_returns_exchange_error() -> None:
    fake_hbt = _FakeHbt(raise_on={"modify"})
    adapter = HftBacktestExecutionAdapter(hbt=fake_hbt, asset_no=0)
    intent = _replace_intent(client_order_id="cid-replace-error")

    execution_errors = adapter.apply_intents([intent])

    assert execution_errors == [(intent, RejectReason.EXCHANGE_ERROR)]


def test_cancel_calls_cancel_with_expected_mapping_and_success_behavior() -> None:
    fake_hbt = _FakeHbt()
    adapter = HftBacktestExecutionAdapter(hbt=fake_hbt, asset_no=5)
    intent = _cancel_intent(client_order_id="cid-cancel-ok")

    execution_errors = adapter.apply_intents([intent])

    assert execution_errors == []
    assert fake_hbt.calls == [
        ("cancel", (5, _to_i64_order_id("cid-cancel-ok"), False))
    ]


def test_cancel_nonzero_result_code_returns_exchange_reject() -> None:
    fake_hbt = _FakeHbt(result_codes={"cancel": 2})
    adapter = HftBacktestExecutionAdapter(hbt=fake_hbt, asset_no=0)
    intent = _cancel_intent(client_order_id="cid-cancel-reject")

    execution_errors = adapter.apply_intents([intent])

    assert execution_errors == [(intent, RejectReason.EXCHANGE_REJECT)]


def test_cancel_exception_returns_exchange_error() -> None:
    fake_hbt = _FakeHbt(raise_on={"cancel"})
    adapter = HftBacktestExecutionAdapter(hbt=fake_hbt, asset_no=0)
    intent = _cancel_intent(client_order_id="cid-cancel-error")

    execution_errors = adapter.apply_intents([intent])

    assert execution_errors == [(intent, RejectReason.EXCHANGE_ERROR)]


def test_to_i64_order_id_numeric_and_deterministic_non_numeric_behavior() -> None:
    assert _to_i64_order_id("42") == 42
    assert _to_i64_order_id("  77 ") == 77

    alpha_a = _to_i64_order_id("cid-alpha")
    alpha_b = _to_i64_order_id("cid-alpha")
    beta = _to_i64_order_id("cid-beta")

    assert alpha_a == alpha_b
    assert alpha_a != beta
    assert 0 <= alpha_a < (1 << 63)
    assert 0 <= beta < (1 << 63)


def test_characterization_scope_excludes_feedback_source_and_fill_ingress_implications() -> None:
    public_methods = {
        name
        for name, member in inspect.getmembers(HftBacktestExecutionAdapter)
        if callable(member) and not name.startswith("_")
    }
    apply_intents_source = inspect.getsource(HftBacktestExecutionAdapter.apply_intents)

    assert "drain_execution_feedback_records" not in public_methods
    assert "ExecutionFeedbackRecordSource" not in apply_intents_source
    assert "FillEvent" not in apply_intents_source

