# Core Runtime

![CI](https://github.com/TradingChassis/core-runtime/actions/workflows/tests.yaml/badge.svg)
![Python](https://img.shields.io/badge/python-3.11+-blue)
![License](https://img.shields.io/badge/license-MIT-green)

Runtime execution layer and orchestration environment for the
[Core (`tradingchassis-core`)](https://github.com/TradingChassis/core).

This repository provides:

- Local execution examples
- Reproducible runtime environments
- Dependency pinning
- [Kubernetes](https://kubernetes.io)-native orchestration via [Argo Workflows](https://argoproj.github.io/workflows)
- CI-integrated build pipelines

---

## 🧠 What is this?

`core-runtime` is the execution and orchestration layer built on top of `tradingchassis-core`.

While Core (`tradingchassis-core`) implements the deterministic trading core,
this repository focuses on:

- how strategies are executed
- how environments are reproduced
- how workloads are orchestrated
- how results are produced and validated

It intentionally contains no domain framework logic.

---

## 🧩 Relationship to Core

```
tradingchassis-core          → core framework, backtesting engine, domain logic
tradingchassis-core-runtime  → executing entrypoints, runtime configs, orchestration
```

The framework is consumed as a pinned Git dependency to guarantee
deterministic runtime environments.

---

## 🏷 Naming Clarification

- Repository/folder name in the monorepo can be `core-runtime`.
- Python import package in this repository is `core_runtime`.
- Distribution/project name in packaging metadata is `tradingchassis-core-runtime`.
- Core package canonical import is `tradingchassis_core`.
- Core distribution/project name is `tradingchassis-core`.

---

## 📁 Repository Structure

```
.github/workflows/    CI pipelines (tests, Argo template deploy)
argo/                 Argo workflow templates
docs/                 Runtime design notes (implementation-facing)
examples/             Alternative example runner/config/strategy paths
scripts/              environment & build helper scripts
core_runtime/         Python runtime entrypoints
tests/                deterministic test data & validation
```

Implementation-facing design notes:

- `docs/venue-adapter-abstraction-design-v1.md`

### Key runtime modules

```
core_runtime/local/            Local execution mode
core_runtime/argo/             Argo workflow entrypoints
core_runtime/strategies/       Example strategies
```

---

## 🚀 Quick Start / Development Setup

### Monorepo workspace layout (recommended for current development)

If your workspace root contains sibling repositories (for example `core/` and
`core-runtime/`), run tests from the workspace root:

```bash
python -m pytest -q core-runtime/tests
python -m pytest -q core/tests/semantics
```

Optional editable installs from workspace root:

```bash
python -m pip install -e core
python -m pip install -e core-runtime
```

### Standalone `core-runtime` repo root

From the `core-runtime` repository root:

```bash
python -m pip install -e .
python -m pytest -q tests
./scripts/check.sh
```

If `tradingchassis_core` is not already available in your environment, install
`core` as a sibling editable package or ensure the pinned dependency resolves.

`PYTHONPATH=.` can be used as a short-term development shortcut, but editable
installation (`python -m pip install -e .`) is the preferred workflow.

---

## 🗺 Entrypoint Matrix

| Mode | Entrypoint | Command shape | Notes |
| --- | --- | --- | --- |
| Local backtest | `core_runtime/local/backtest.py` | `python -m core_runtime.local.backtest --config core_runtime/local/local.json` | Main local runner. |
| Argo plan/run orchestration | `core_runtime/backtest/runtime/entrypoint.py` | `python -m core_runtime.backtest.runtime.entrypoint --config core_runtime/argo/argo.json --plan` | Planner and sweep-context emitter for Argo flow. |
| Sweep worker | `core_runtime/backtest/runtime/run_sweep.py` | `python -m core_runtime.backtest.runtime.run_sweep --context <path-to-sweep-json>` | Executes one sweep context (pod-level unit). |
| Examples path | `examples/local/backtest.py` | `python examples/local/backtest.py --config examples/local/local.json` | Alternative example path; useful for reference but duplicates runtime patterns. |

Use `core_runtime/local/*` for local runtime development, `core_runtime/backtest/runtime/*`
for Argo workflow execution, and `examples/*` as a duplicate reference path.

---

## ⚠️ Local Config Path Caveat

Current shipped local JSON configs use cwd-relative paths for
`tests/data/...` inputs and `.runtime/...` outputs.

Supported workflow: run local commands from the `core-runtime` repository root.
If you run from a different cwd, adjust config paths accordingly.

---

## ✅ Current usability status (local hftbacktest path)

The current local backtest path is verified and usable from the `core-runtime`
repository root.

Verified local workflow:

```bash
python -m pip install -e .
python -m core_runtime.local.backtest --config core_runtime/local/local.json
```

Verified output location:

```
.runtime/local/results/events.json
.runtime/local/results/stats.npz
```

Verified tests:

```bash
python -m pytest -q tests
python -m pytest -q core-runtime/tests
python -m pytest -q core/tests/semantics
```

Current caveats:

- Paths are cwd-relative; supported workflow is running from `core-runtime` root.
- hftbacktest timestamp-ordering warnings may appear from fixture ordering but do not fail the run.
- `tests/data/results/` may contain historical/sample artifacts and is no longer the default local output location.
- Naming is aligned (`core-runtime` repo, `tradingchassis-core-runtime` distribution, `core_runtime` import package, `tradingchassis_core` core import package).

This status confirms local usability for the current local hftbacktest path; it
does not imply full canonical Event Stream completion.

---

## 📌 Current semantic status (transitional)

`core-runtime` is currently usable as a transitional runtime around `core`:

- canonical `MarketEvent`, `OrderSubmittedEvent`, and `ControlTimeEvent` paths are in use
- post-submission order/fill progression remains on the snapshot-compatibility path
- `FillEvent` runtime ingress remains deferred

For adapter boundary context, see:

- `docs/venue-adapter-abstraction-design-v1.md`

---

## 📌 Dependency Pinning & Reproducibility

The `tradingchassis-core` dependency is pinned by commit SHA.

Create a `.env` file:

```bash
TRADINGCHASSIS_CORE_COMMIT=<commit-sha>
```

Generate reproducible environments:

```bash
./scripts/compile-requirements.sh
```

This produces:

- `requirements.txt`
- `requirements-dev.txt`

These files are used by:

- Dev Containers
- Docker images

---

## ▶️ Local Execution

Run a deterministic local backtest:

```bash
python -m core_runtime.local.backtest \
  --config core_runtime/local/local.json
```

This uses synthetic deterministic test data located in:

```
tests/data/parts/
```

Results are written to:

```
.runtime/local/results/
```

Important: `core_runtime/local/local.json` and `examples/local/local.json`
use cwd-relative paths. Run from the `core-runtime` repository root, or adjust
config paths for your current working directory.

---

## ⚙️ Infrastructure Requirements

The Argo-based workflows require:

- A self-hosted GitHub Actions runner
- microk8s Kubernetes distribution (with sudo access)
- Argo Workflows installed in the cluster
- GitHub Container Registry access (GHCR_TOKEN secret)

GitHub-hosted runners are only used for unit tests.
All Kubernetes orchestration runs on self-hosted infrastructure.

---

## ☸ Kubernetes & Argo Workflows

This runtime is designed for Kubernetes-native execution using Argo Workflows.

Two core workflow templates define the execution pipeline:

```
argo/workflowtemplate-build-push-ghcr.yaml
argo/workflowtemplate-backtest.yaml
```

### 🐳 Runtime Image Build & Push

`workflowtemplate-build-push-ghcr.yaml` builds the Core Runtime Docker image and pushes it to
GitHub Container Registry (GHCR).

This image contains:

- Python dependencies and entrypoints
- tradingchassis-core and core-runtime commit SHA
- strategies and configs

It acts as an immutable and deterministic runtime environment for all backtests.

### ▶️ Backtest Orchestration

`workflowtemplate-backtest.yaml` orchestrates backtest workloads using Argo.

It:

- pulls the runtime image from GHCR
- executes runtime entrypoints inside Kubernetes pods
- distributes workloads across the cluster
- saves deterministic result artifacts

All backtests always run inside the runtime image.

### 🔄 End-to-End Flow

```
Docker build → Push to GHCR → Argo pulls image → Backtests execute in cluster
```

This guarantees:

- identical runtime environments locally and in Kubernetes
- reproducible research runs

---

## 🔐 GHCR Registry Access

To allow Kubernetes to pull runtime images from GitHub Container Registry (GHCR),
the deployment workflow creates a `docker-registry` secret inside the target Kubernetes namespace.

The secret is created by the GitHub Actions workflow located at:

```
.github/workflows/deploy_argo_template.yaml
```

It runs the equivalent of:

```bash
sudo microk8s kubectl -n $K8S_NAMESPACE create secret docker-registry ghcr-secret \
  --docker-server=ghcr.io \
  --docker-username=git \
  --docker-password=$GHCR_TOKEN \
  --dry-run=client -o yaml | sudo microk8s kubectl apply -f -
```

### Required Repository Secret

The workflow requires a GitHub repository secret named:

```
GHCR_TOKEN
```

This token must be a GitHub Personal Access Token with:

* `read:packages`

Add it under:

```
Repository → Settings → Secrets and variables → Actions
```

Without this secret, the workflow cannot authenticate against GHCR, and Kubernetes will fail to pull the runtime image.

---

## 🛠 Scripts

| Script                    | Purpose                                         |
| ------------------------- | ----------------------------------------------- |
| `compile-requirements.sh` | Pins tradingchassis-core and resolves dependencies |
| `post-create.sh`          | Dev container bootstrap                         |
| `check.sh`                | Local validation helpers                        |

---

## 🧪 Test Data

Synthetic datasets are provided in:

```
tests/data/parts/
```

Historical/sample result artifacts may exist in:

```
tests/data/results/
```

Default local backtest outputs are now written to:

```
.runtime/local/results/
```

Helper generation scripts:

```
tests/data/scripts/
```

These guarantee reproducible runtime validation.

---

## 🧪 CI & Automation

GitHub Actions workflows:

- `tests.yaml` — runtime validation
- `deploy_argo_template.yaml` — Argo template deployment

Supports both GitHub-hosted and self-hosted runners respectively.

---

## 🎯 Design Principles

- Determinism over convenience
- Reproducible environments
- Explicit execution entrypoints
- Infrastructure separated from domain logic
- Cloud-native orchestration

---

## 📌 Scope

This repository includes:

- runtime execution logic
- environment orchestration
- CI pipelines
- container workflows

It does not include:

- Core internals
- specific strategy research logic

---

## 🏷️ Versioning

This project follows the MIT license and semantic versioning.
Initial public release: `v0.1.0`
