from __future__ import annotations

import pytest
from tradingchassis_core.core.domain.configuration import CoreConfiguration

from core_runtime.backtest.runtime.core_configuration_mapper import (
    build_core_configuration_from_run_config,
)


def _valid_run_config() -> dict[str, object]:
    return {
        "engine": {
            "instrument": "BTC_USDC-PERPETUAL",
            "tick_size": 0.1,
            "lot_size": 0.01,
            "contract_size": 1.0,
        },
        "core": {
            "version": "v1",
            "market": {
                "instruments": {
                    "BTC_USDC-PERPETUAL": {
                        "tick_size": 0.1,
                        "lot_size": 0.01,
                        "contract_size": 1.0,
                    }
                }
            },
        },
    }


def test_valid_explicit_core_builds_core_configuration() -> None:
    cfg = build_core_configuration_from_run_config(_valid_run_config())

    assert isinstance(cfg, CoreConfiguration)
    assert cfg.version == "v1"
    assert cfg.payload["market"]["instruments"]["BTC_USDC-PERPETUAL"]["tick_size"] == 0.1


def test_missing_core_fails() -> None:
    run_config = _valid_run_config()
    run_config.pop("core")

    with pytest.raises(ValueError, match="Missing required top-level section: core"):
        build_core_configuration_from_run_config(run_config)


def test_missing_version_fails() -> None:
    run_config = _valid_run_config()
    run_config["core"] = {
        "market": run_config["core"]["market"],  # type: ignore[index]
    }

    with pytest.raises(ValueError, match="core.version"):
        build_core_configuration_from_run_config(run_config)


def test_missing_market_instruments_fails() -> None:
    run_config = _valid_run_config()
    run_config["core"] = {"version": "v1", "market": {}}

    with pytest.raises(ValueError, match="core.market.instruments"):
        build_core_configuration_from_run_config(run_config)


def test_missing_instrument_entry_fails() -> None:
    run_config = _valid_run_config()
    run_config["core"] = {
        "version": "v1",
        "market": {
            "instruments": {
                "ETH_USDC-PERPETUAL": {
                    "tick_size": 0.1,
                    "lot_size": 0.01,
                    "contract_size": 1.0,
                }
            }
        },
    }

    with pytest.raises(ValueError, match="core.market.instruments.BTC_USDC-PERPETUAL"):
        build_core_configuration_from_run_config(run_config)


@pytest.mark.parametrize("field_name", ["tick_size", "lot_size", "contract_size"])
def test_missing_required_metadata_field_fails(field_name: str) -> None:
    run_config = _valid_run_config()
    instrument_cfg = run_config["core"]["market"]["instruments"]["BTC_USDC-PERPETUAL"]  # type: ignore[index]
    instrument_cfg.pop(field_name)

    with pytest.raises(ValueError, match=field_name):
        build_core_configuration_from_run_config(run_config)


@pytest.mark.parametrize("field_name", ["tick_size", "lot_size", "contract_size"])
def test_none_value_fails(field_name: str) -> None:
    run_config = _valid_run_config()
    run_config["core"]["market"]["instruments"]["BTC_USDC-PERPETUAL"][field_name] = None  # type: ignore[index]

    with pytest.raises(ValueError, match=field_name):
        build_core_configuration_from_run_config(run_config)


@pytest.mark.parametrize("field_name", ["tick_size", "lot_size", "contract_size"])
def test_bool_value_fails(field_name: str) -> None:
    run_config = _valid_run_config()
    run_config["core"]["market"]["instruments"]["BTC_USDC-PERPETUAL"][field_name] = True  # type: ignore[index]

    with pytest.raises(TypeError, match="must be numeric"):
        build_core_configuration_from_run_config(run_config)


@pytest.mark.parametrize("field_name", ["tick_size", "lot_size", "contract_size"])
def test_non_numeric_value_fails(field_name: str) -> None:
    run_config = _valid_run_config()
    run_config["core"]["market"]["instruments"]["BTC_USDC-PERPETUAL"][field_name] = "x"  # type: ignore[index]

    with pytest.raises(TypeError, match="must be numeric"):
        build_core_configuration_from_run_config(run_config)


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf")])
def test_non_finite_value_fails(bad: float) -> None:
    run_config = _valid_run_config()
    run_config["core"]["market"]["instruments"]["BTC_USDC-PERPETUAL"]["tick_size"] = bad  # type: ignore[index]

    with pytest.raises(ValueError, match="must be finite"):
        build_core_configuration_from_run_config(run_config)


@pytest.mark.parametrize("bad", [0.0, -1.0])
def test_non_positive_value_fails(bad: float) -> None:
    run_config = _valid_run_config()
    run_config["core"]["market"]["instruments"]["BTC_USDC-PERPETUAL"]["tick_size"] = bad  # type: ignore[index]

    with pytest.raises(ValueError, match="must be > 0"):
        build_core_configuration_from_run_config(run_config)


def test_no_fallback_from_engine_when_core_missing() -> None:
    run_config = {
        "engine": {
            "instrument": "BTC_USDC-PERPETUAL",
            "tick_size": 0.1,
            "lot_size": 0.01,
            "contract_size": 1.0,
        }
    }

    with pytest.raises(ValueError, match="Missing required top-level section: core"):
        build_core_configuration_from_run_config(run_config)


def test_engine_duplicate_exact_match_allowed() -> None:
    run_config = _valid_run_config()

    cfg = build_core_configuration_from_run_config(run_config)

    assert isinstance(cfg, CoreConfiguration)


@pytest.mark.parametrize("field_name", ["tick_size", "lot_size", "contract_size"])
def test_engine_duplicate_mismatch_fails(field_name: str) -> None:
    run_config = _valid_run_config()
    run_config["engine"][field_name] = 999.0  # type: ignore[index]

    with pytest.raises(ValueError, match="Conflicting duplicate field values"):
        build_core_configuration_from_run_config(run_config)
