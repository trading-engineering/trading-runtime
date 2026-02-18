# Trading Runtime

![CI](https://github.com/trading-engineering/trading-runtime/actions/workflows/tests.yaml/badge.svg)
![Python](https://img.shields.io/badge/python-3.11+-blue)
![License](https://img.shields.io/badge/license-MIT-green)

Runtime execution layer and orchestration environment for the
[trading-platform](https://github.com/trading-engineering/trading-platform)
framework.

This repository provides:

- Local execution examples
- Reproducible runtime environments
- Dependency pinning
- [Kubernetes](https://kubernetes.io)-native orchestration via [Argo Workflows](https://argoproj.github.io/workflows)
- CI-integrated build pipelines

---

## 🧠 What is this?

`trading-runtime` is the execution and orchestration layer built on top of
`trading-platform`.

While `trading-platform` implements the deterministic trading framework,
this repository focuses on:

- how strategies are executed
- how environments are reproduced
- how workloads are orchestrated
- how results are produced and validated

It intentionally contains no domain framework logic.

---

## 🧩 Relationship to trading-platform

```
trading-platform  → core framework, backtesting engine, domain logic
trading-runtime   → executing entrypoints, runtime configs, orchestration
```

The platform is consumed as a pinned Git dependency to guarantee
deterministic runtime environments.

---

## 📁 Repository Structure

```

.github/workflows/    CI pipelines (tests, Argo template deploy)
argo/                 Argo workflow templates
scripts/              environment & build helper scripts
trading_runtime/      Python runtime entrypoints
tests/                deterministic test data & validation

```

### Key runtime modules

```

trading_runtime/local/         Local execution mode
trading_runtime/argo/          Argo workflow entrypoints
trading_runtime/strategies/    Example strategies

````

## 📌 Dependency Pinning & Reproducibility

The `trading-platform` dependency is pinned by commit SHA.

Create a `.env` file:

```bash
TRADING_PLATFORM_COMMIT=<commit-sha>
````

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
python trading_runtime/local/backtest.py \
  --config trading_runtime/local/local.json
```

This uses synthetic deterministic test data located in:

```
tests/data/parts/
```

Results are written to:

```
tests/data/results/
```

---

## ☸ Kubernetes & Argo Workflows

This runtime is designed for Kubernetes-native execution using Argo Workflows.

Two core workflow templates define the execution pipeline:

```
argo/workflowtemplate-build-push-ghcr.yaml
argo/workflowtemplate-backtest.yaml
```

### 🐳 Runtime Image Build & Push

`workflowtemplate-build-push-ghcr.yaml` builds the trading-runtime Docker image and pushes it to
GitHub Container Registry (GHCR).

This image contains:

- Python dependencies and entrypoints
- trading-platform and trading-runtime commit SHA
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
| `compile-requirements.sh` | Pins trading-platform and resolves dependencies |
| `post-create.sh`          | Dev container bootstrap                         |
| `check.sh`                | Local validation helpers                        |

---

## 🧪 Test Data

Synthetic datasets are provided in:

```
tests/data/parts/
```

Result artifacts:

```
tests/data/results/
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

- trading framework internals
- specific strategy research logic

---

## 🏷️ Versioning

This project follows the MIT license and semantic versioning.
Initial public release: `v0.1.0`
