from __future__ import annotations

import json
import sys
import types
from pathlib import Path

import pytest
from tradingchassis_core.core.domain.configuration import CoreConfiguration

from core_runtime.local.backtest import load_config


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _load_sample_config(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _install_oci_stubs(monkeypatch: pytest.MonkeyPatch) -> None:
    oci_mod = types.ModuleType("oci")
    auth_mod = types.ModuleType("oci.auth")
    signers_mod = types.ModuleType("oci.auth.signers")
    config_mod = types.ModuleType("oci.config")
    object_storage_mod = types.ModuleType("oci.object_storage")
    signer_mod = types.ModuleType("oci.signer")

    class _InstancePrincipalsSecurityTokenSigner:  # pragma: no cover - stub only
        pass

    class _ObjectStorageClient:  # pragma: no cover - stub only
        pass

    class _Signer:  # pragma: no cover - stub only
        pass

    def _from_file(*, file_location: str, profile_name: str) -> dict[str, object]:
        _ = (file_location, profile_name)
        return {}

    signers_mod.InstancePrincipalsSecurityTokenSigner = _InstancePrincipalsSecurityTokenSigner
    config_mod.from_file = _from_file
    object_storage_mod.ObjectStorageClient = _ObjectStorageClient
    signer_mod.Signer = _Signer

    monkeypatch.setitem(sys.modules, "oci", oci_mod)
    monkeypatch.setitem(sys.modules, "oci.auth", auth_mod)
    monkeypatch.setitem(sys.modules, "oci.auth.signers", signers_mod)
    monkeypatch.setitem(sys.modules, "oci.config", config_mod)
    monkeypatch.setitem(sys.modules, "oci.object_storage", object_storage_mod)
    monkeypatch.setitem(sys.modules, "oci.signer", signer_mod)


def test_local_loader_fails_early_when_core_missing(tmp_path: Path) -> None:
    sample_path = _repo_root() / "core_runtime/local/bt_config_local.json"
    config = _load_sample_config(sample_path)
    config.pop("core", None)

    test_path = tmp_path / "missing-core.json"
    test_path.write_text(json.dumps(config), encoding="utf-8")

    with pytest.raises(ValueError, match="Missing required top-level section: core"):
        load_config(str(test_path))


def test_local_loader_succeeds_with_valid_core() -> None:
    sample_path = _repo_root() / "core_runtime/local/bt_config_local.json"
    cfg = load_config(str(sample_path))

    assert isinstance(cfg.core_cfg, CoreConfiguration)
    assert cfg.core_cfg.version == "v1"


def test_argo_entrypoint_rejects_invalid_run_config_before_planning(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_oci_stubs(monkeypatch)

    from core_runtime.backtest.runtime.entrypoint import main as argo_entrypoint_main

    sample_path = _repo_root() / "core_runtime/argo/bt_config_argo.json"
    config = _load_sample_config(sample_path)
    config.pop("core", None)

    config_path = tmp_path / "argo-missing-core.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "entrypoint.py",
            "--config",
            str(config_path),
            "--plan",
        ],
    )

    with pytest.raises(ValueError, match="Missing required top-level section: core"):
        argo_entrypoint_main()


def test_argo_sweep_worker_rejects_context_missing_core(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_oci_stubs(monkeypatch)

    from core_runtime.backtest.runtime.run_sweep import main as run_sweep_main

    context = {
        "experiment_id": "exp-1",
        "segment_id": "seg-1",
        "sweep_id": "sweep-1",
        "stage": "derived",
        "venue": "deribit",
        "datatype": "mixed",
        "symbol": "BTC_USDC-PERPETUAL",
        "file_keys": [],
        "parameters": {
            "engine": {
                "instrument": "BTC_USDC-PERPETUAL",
            },
            "strategy": {},
            "risk": {},
        },
        "scratch_root": str(tmp_path / "scratch"),
        "results_root": str(tmp_path / "results"),
    }

    context_path = tmp_path / "context.json"
    context_path.write_text(json.dumps(context), encoding="utf-8")

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_sweep.py",
            "--context",
            str(context_path),
            "--scratch-root",
            str(tmp_path / "scratch"),
        ],
    )

    with pytest.raises(ValueError, match="Missing required top-level section: core"):
        run_sweep_main()


def test_argo_emit_includes_core_section_in_sweep_context(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_oci_stubs(monkeypatch)

    from core_runtime.backtest.orchestrator.planner_models import (
        ExperimentPlan,
        SegmentPlan,
    )
    from core_runtime.backtest.orchestrator.sweeps import SweepPlan
    from core_runtime.backtest.runtime.entrypoint import _emit_sweep_context

    plan = ExperimentPlan(
        experiment_id="exp-1",
        segments=[
            SegmentPlan(
                segment_id="seg-1",
                start_ts_ns=1,
                end_ts_ns=2,
                estimated_bytes=123,
                files=["file-1.npz"],
                sweeps=[SweepPlan(sweep_id="sweep-0000", parameters={})],
            )
        ],
    )
    base_cfg = {
        "experiment": {
            "venue": "deribit",
            "datatype": "mixed",
            "symbol": "BTC_USDC-PERPETUAL",
        },
        "engine": {"instrument": "BTC_USDC-PERPETUAL"},
        "strategy": {"class_path": "x:y"},
        "risk": {"scope": "s", "notional_limits": {"currency": "USDC", "max_gross_notional": 1.0}},
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

    _emit_sweep_context(
        plan=plan,
        base_cfg=base_cfg,
        scratch_root=tmp_path / "scratch",
        results_root=tmp_path / "results",
        out_dir=tmp_path / "emit",
    )

    emitted = json.loads(
        (tmp_path / "emit" / "seg-1__sweep-0000.json").read_text(encoding="utf-8")
    )
    assert emitted["parameters"]["core"] == base_cfg["core"]
