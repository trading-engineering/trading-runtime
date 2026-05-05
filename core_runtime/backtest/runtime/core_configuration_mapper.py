from __future__ import annotations

import math
from collections.abc import Collection, Mapping

from tradingchassis_core.core.domain.configuration import CoreConfiguration

_REQUIRED_METADATA_FIELDS = ("tick_size", "lot_size", "contract_size")


def _require_mapping(value: object, *, field_path: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{field_path} must be a mapping")

    normalized: dict[str, object] = {}
    for key, nested in value.items():
        if not isinstance(key, str):
            raise TypeError(f"{field_path} keys must be strings")
        normalized[key] = nested
    return normalized


def _require_non_empty_string(value: object, *, field_path: str) -> str:
    if value is None:
        raise ValueError(f"Missing required field: {field_path}")
    if not isinstance(value, str):
        raise TypeError(f"{field_path} must be a string")
    if not value:
        raise ValueError(f"{field_path} must be non-empty")
    return value


def _require_positive_number(value: object, *, field_path: str) -> float:
    if value is None:
        raise ValueError(f"Missing required field: {field_path}")
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{field_path} must be numeric")

    numeric = float(value)
    if not math.isfinite(numeric):
        raise ValueError(f"{field_path} must be finite")
    if numeric <= 0.0:
        raise ValueError(f"{field_path} must be > 0")
    return numeric


def _validate_instrument_metadata(
    *,
    instruments: Mapping[str, object],
    instrument: str,
) -> dict[str, float]:
    instrument_raw = instruments.get(instrument)
    if instrument_raw is None:
        raise ValueError(
            "Missing required core instrument entry: "
            f"core.market.instruments.{instrument}"
        )

    instrument_cfg = _require_mapping(
        instrument_raw,
        field_path=f"core.market.instruments.{instrument}",
    )

    validated: dict[str, float] = {}
    for field in _REQUIRED_METADATA_FIELDS:
        validated[field] = _require_positive_number(
            instrument_cfg.get(field),
            field_path=f"core.market.instruments.{instrument}.{field}",
        )
    return validated


def build_core_configuration_from_sections(
    *,
    core_section: Mapping[str, object],
    engine_section: Mapping[str, object] | None = None,
    processed_instruments: Collection[str] | None = None,
) -> CoreConfiguration:
    core = _require_mapping(core_section, field_path="core")
    version = _require_non_empty_string(core.get("version"), field_path="core.version")

    market_raw = core.get("market")
    if market_raw is None:
        raise ValueError("Missing required field: core.market")
    market = _require_mapping(market_raw, field_path="core.market")

    instruments_raw = market.get("instruments")
    if instruments_raw is None:
        raise ValueError("Missing required field: core.market.instruments")
    instruments = _require_mapping(
        instruments_raw,
        field_path="core.market.instruments",
    )

    if not instruments:
        raise ValueError("core.market.instruments must contain at least one instrument")

    to_validate = set(instruments.keys())
    if processed_instruments is not None:
        to_validate.update(processed_instruments)

    validated_core_values: dict[str, dict[str, float]] = {}
    for instrument in sorted(to_validate):
        validated_core_values[instrument] = _validate_instrument_metadata(
            instruments=instruments,
            instrument=instrument,
        )

    if engine_section is not None:
        engine = _require_mapping(engine_section, field_path="engine")
        instrument_raw = engine.get("instrument")
        if instrument_raw is not None:
            instrument = _require_non_empty_string(
                instrument_raw,
                field_path="engine.instrument",
            )
            if instrument not in instruments:
                raise ValueError(
                    "engine.instrument must exist in core.market.instruments: "
                    f"{instrument}"
                )

            core_values = validated_core_values[instrument]
            for field in _REQUIRED_METADATA_FIELDS:
                if field not in engine:
                    continue
                engine_value = _require_positive_number(
                    engine[field],
                    field_path=f"engine.{field}",
                )
                if engine_value != core_values[field]:
                    raise ValueError(
                        f"Conflicting duplicate field values for {field}: "
                        f"engine.{field}={engine_value} != "
                        f"core.market.instruments.{instrument}.{field}={core_values[field]}"
                    )

    # CoreConfiguration is constructed from the explicit core section only.
    payload = {k: v for k, v in core.items() if k != "version"}
    return CoreConfiguration(version=version, payload=payload)


def build_core_configuration_from_run_config(
    run_config: Mapping[str, object],
) -> CoreConfiguration:
    config = _require_mapping(run_config, field_path="run_config")
    if "core" not in config:
        raise ValueError("Missing required top-level section: core")

    core_section = _require_mapping(config["core"], field_path="core")

    engine_section: Mapping[str, object] | None = None
    processed_instruments: list[str] = []
    if "engine" in config:
        engine_section = _require_mapping(config["engine"], field_path="engine")
        if "instrument" in engine_section:
            instrument = _require_non_empty_string(
                engine_section["instrument"],
                field_path="engine.instrument",
            )
            processed_instruments.append(instrument)

    return build_core_configuration_from_sections(
        core_section=core_section,
        engine_section=engine_section,
        processed_instruments=processed_instruments or None,
    )
