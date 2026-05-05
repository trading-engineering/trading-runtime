# Changelog

All notable changes to this project will be documented in this file.
This project adheres to Semantic Versioning.

## [Unreleased]

## [0.1.0] – 2026-02-17

Initial public release of the Core Runtime execution and orchestration layer.

### Added

#### Runtime Execution

- Local deterministic backtest entrypoint
- Argo workflow execution entrypoints
- Structured runtime configuration model
- Strategy execution adapters

#### Dependency Management

- Commit-pinned `tradingchassis-core` integration
- Reproducible dependency compilation via pip-tools
- Environment bootstrap scripts

#### Containerization

- Deterministic Docker runtime image
- GHCR push workflow template
- Immutable runtime environment definition

#### Kubernetes & Orchestration

- Argo WorkflowTemplates for:
  - Image build & push
  - Backtest orchestration
- microk8s-compatible deployment model
- Namespace-based secret management for GHCR

#### CI & Infrastructure

- GitHub-hosted test workflow
- Self-hosted runner for Kubernetes deployment
- Automated Argo template deployment pipeline

#### Reproducibility

- Synthetic deterministic test datasets
- Result artifact validation structure
- Environment parity between local and cluster execution

#### Tooling

- Dev container bootstrap
- Validation helpers
- Dependency compilation script
