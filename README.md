# TradingChassis — Core Runtime

![CI](https://github.com/TradingChassis/core-runtime/actions/workflows/tests.yaml/badge.svg)
![Python](https://img.shields.io/badge/python-3.11+-blue)
![License](https://img.shields.io/badge/license-MIT-green)

Execution and orchestration environment around Core.

Core Runtime consumes Core (`tradingchassis_core`) and provides local/cluster entrypoints,
configuration, adapter integration, runtime packaging, and reproducible execution workflows.

---

## Overview

Core Runtime is the runtime layer for executing Core semantics in concrete environments.

- local hftbacktest-backed backtest execution
- runtime entrypoints for orchestration flows
- reproducible dependency/runtime packaging
- CI and infrastructure wiring for deployment workflows

---

## What Core Runtime is

Core Runtime provides:

- executable runtime entrypoints (`core_runtime/...`)
- runtime configs and environment wiring
- adapter-facing integration layers around Core
- orchestration integration (Argo/Kubernetes)
- runtime validation and smoke/test workflows

---

## What Core Runtime is not

Core Runtime is not the semantic source of truth for Core concepts.

It consumes Core and should not redefine canonical terms such as Event, Event Stream, Processing
Order, State, or Risk Engine.

---

## Current local hftbacktest usability status

Current local smoke is usable from the `core-runtime` repository root:

```bash
python -m core_runtime.local.backtest --config core_runtime/local/bt_config_local.json
```

Default output location:

```text
.runtime/local/results/
```

This confirms current local usability and does not claim full canonical Event Stream completion.

---

## Quick start

From the `core-runtime` repository root:

```bash
python -m pip install -e .
python -m core_runtime.local.backtest --config core_runtime/local/bt_config_local.json
```

---

## Entrypoint matrix

| Mode | Entrypoint | Command shape | Notes |
| --- | --- | --- | --- |
| Local backtest | `core_runtime/local/backtest.py` | `python -m core_runtime.local.backtest --config core_runtime/local/bt_config_local.json` | Main local runner. |
| Argo plan/run orchestration | `core_runtime/backtest/runtime/entrypoint.py` | `python -m core_runtime.backtest.runtime.entrypoint --config core_runtime/argo/bt_config_argo.json --plan` | Planner and sweep-context emitter for Argo flow. |
| Sweep worker | `core_runtime/backtest/runtime/run_sweep.py` | `python -m core_runtime.backtest.runtime.run_sweep --context <path-to-sweep-json>` | Executes one sweep context. |

---

## Adapter capability model

| Capability area | Status | Notes |
| --- | --- | --- |
| Canonical runtime paths | Active | `MarketEvent`, `OrderSubmittedEvent`, `ControlTimeEvent` |
| Runtime-local compatibility handling | Active | Raw venue order snapshots stay in runtime bookkeeping; Core receives canonical `OrderExecutionFeedbackEvent` (account-level only). |
| Deferred capabilities | Deferred | Runtime `FillEvent` ingress, `ExecutionFeedbackRecordSource`, replay/storage/Event Stream persistence, `ProcessingContext` |

---

## Current hftbacktest capability map

- Local hftbacktest flow is usable for current transitional runtime paths.
- Compatibility mechanisms remain in place for post-submission progression.
- Deferred capabilities are intentionally not presented as shipped runtime behavior.

---

## Canonical runtime paths

- `MarketEvent`
- `OrderSubmittedEvent`
- `ControlTimeEvent`

---

## Runtime-local compatibility handling

- snapshot-based post-submission bookkeeping remains runtime-local
- Core ingestion uses account-level `OrderExecutionFeedbackEvent`
- no snapshot row payload is pushed into Core

---

## Deferred capabilities

- runtime `FillEvent` ingress
- `ExecutionFeedbackRecordSource`
- replay/storage/Event Stream persistence
- `ProcessingContext`

---

## Package and import names

- Human-facing concept name: Core Runtime
- Distribution/project name: `tradingchassis-core-runtime`
- Python import package: `core_runtime`
- Core distribution/project name: `tradingchassis-core`
- Core Python import package: `tradingchassis_core`

---

## Repository structure

```text
.github/workflows/          CI and deployment workflows
.github/argo-launchers/     Argo Workflow submit wrappers used by GitHub Actions
argo/templates/             Argo WorkflowTemplates shown in Argo UI
core_runtime/               Runtime entrypoints and execution modules
docs/                       Runtime implementation notes
scripts/                    Build/validation helper scripts
tests/                      Runtime tests and deterministic fixtures
```

---

## Configuration

Primary local config:

- `core_runtime/local/bt_config_local.json`
- OCI config template (for local object storage auth setups): `core_runtime/local/oci.config.example`

Note: local JSON configs use cwd-relative paths for `tests/data/...` inputs and `.runtime/...`
outputs. The supported default workflow is to run commands from the `core-runtime` repo root.

---

## Development setup

### Standalone `core-runtime` root

```bash
python -m pip install -e .
python -m pytest -q tests
./scripts/check.sh
```

### Monorepo workspace root (with `core/` and `core-runtime/`)

```bash
python -m pip install -e core
python -m pip install -e core-runtime
python -m pytest -q core-runtime/tests
python -m pytest -q core/tests
```

---

## Test commands

From `core-runtime` root:

```bash
python -m pytest -q tests
./scripts/check.sh
```

From monorepo root:

```bash
python -m pytest -q core-runtime/tests
python -m pytest -q core/tests
```

---

## Relationship to Core

Core provides deterministic semantics and domain contracts.

Core Runtime provides execution environments and orchestration around those semantics.

---

## Dependency pinning and reproducibility

Core dependency can be pinned by commit SHA through environment configuration:

```bash
TRADINGCHASSIS_CORE_COMMIT=<commit-sha>
```

To compile reproducible requirements:

```bash
./scripts/compile-requirements.sh
```

Artifacts:

- `requirements.txt`
- `requirements-dev.txt`

---

## Infrastructure notes

Argo WorkflowTemplates (visible in Argo UI) are defined in:

- `argo/templates/workflowtemplate-build-push-ghcr.yaml`
- `argo/templates/workflowtemplate-backtest-fanout.yaml`

GitHub-only Argo submit wrappers are in:

- `.github/argo-launchers/run-build.yaml`
- `.github/argo-launchers/run-backtest.yaml`

Automation that applies templates and starts workflows is in:

- `.github/workflows/argo-build-and-backtest.yaml`

### Argo UI usage

Use this model to avoid confusion:

- `argo/templates/*`: reusable `WorkflowTemplate` definitions that appear in the Argo UI.
- `.github/argo-launchers/*`: one-off `Workflow` manifests used by GitHub Actions with `envsubst`.

Namespace intent:

- `dev`: branch and development runs.
- `prod`: main branch and production-like runs.

#### Build image from Argo UI (`build-push-ghcr`)

Template: `build-push-ghcr`

Recommended parameters:

- `git_repo`: keep default `https://github.com/TradingChassis/core-runtime.git`.
- `image_repo`: keep default `ghcr.io/tradingchassis/core-runtime`.
- `git_branch`: set the branch name for tagging (default `main`).
- `core_runtime_commit`: set to a real commit SHA (required).

Guardrails:

- `core_runtime_commit` must be a 7-40 character hex SHA.
- `git_repo` must be an HTTPS URL ending in `.git`.

Tagging behavior:

- always pushes `<image_repo>:<branch-tag>`
- always pushes `<image_repo>:<commit-sha>`
- also pushes `<image_repo>:latest` when `git_branch=main`

#### Run backtest from Argo UI (`backtest-fanout`)

Template: `backtest-fanout`

Recommended parameters:

- `image_repo`: keep default.
- `image_tag`: set to the exact commit SHA built by `build-push-ghcr` for reproducibility.
- `experiment_config`: keep default unless intentionally testing a different in-image config.
- `scratch_root`: keep default `/mnt/scratch`.

Guardrails:

- prefer commit SHA tags for `prod` runs.
- use mutable tags such as `latest` only for quick smoke checks.

### Backtest storage vs MLflow tracking

Core Runtime and MLflow serve different purposes in cluster runs:

- Backtest output artifacts are written by Core Runtime directly to OCI Object Storage.
- MLflow is used for tracking metadata only (params, metrics, tags), not for artifact files.

Backtest artifact storage path:

- bucket: `data`
- prefix: `backtests/<experiment_id>/...`
- auth mode: OCI Instance Principals (IAM policy controlled)

Code anchors:

- backtest result download/upload pipeline: `core_runtime/backtest/runtime/run_sweep.py`
- OCI Object Storage adapter + auth behavior: `core_runtime/backtest/io/s3_adapter.py`
- MLflow tracking logger (no artifact logging): `core_runtime/backtest/runtime/mlflow_segment_logger.py`

Tracking-only policy:

- MLflow run metadata remains in the backend store.
- MLflow artifact storage is intentionally unsupported in this setup.
- If a client starts calling artifact APIs (for example `mlflow.log_artifact(...)`), treat failures as expected until artifact storage is intentionally added.

---

## Scripts

| Script | Purpose |
| --- | --- |
| `compile-requirements.sh` | Resolves dependencies and pins Core revision inputs |
| `post-create.sh` | Dev container bootstrap |
| `check.sh` | Local validation helpers |

---

## Documentation index

- Runtime adapter design: `docs/venue-adapter-abstraction-design-v1.md`
- Shared terminology source of truth: `docs/docs/00-guides/terminology.md`
- Core library scope: `core/README.md`

---

## License and versioning

MIT licensed. Versioning follows semantic versioning.
