# Documentation Map

This docs set tracks the current codebase state after the DAG schedule, schema,
idempotency, historical-indicator, recommendation-model, Superset dashboard,
and dashboard-platform migration work.

## Core Documents

- [README](../README.md)
  Repository-level overview and quick start.

- [1. System Overview](./1_SYSTEM_OVERVIEW.md)
  What the project currently does, current limitations, and current workflow.

- [2. Technical Architecture](./2_TECHNICAL_ARCHITECTURE.md)
  Live models, pipeline stages, adapters, and known architecture gaps.

- [3. Deployment Guide](./3_DEPLOYMENT_GUIDE.md)
  Docker services, Airflow DAGs, ports, and operational notes.

- [4. User Guide](./4_USER_GUIDE.md)
  How to run the system today, what to trust, and what is still in transition.

## ADRs

- [ADR-001: Current Canonical Architecture and Supported Interfaces](./adr/ADR-001-current-canonical-architecture-and-supported-interfaces.md)
  Establishes the current architectural source of truth for the repo during cleanup and refactor.
- [ADR-002: Canonical Daily Price Schema](./adr/ADR-002-canonical-daily-price-schema.md)
  Defines the current production truth for daily price records as a close-price-centric schema.
- [ADR-003: Current Data Source Strategy](./adr/ADR-003-current-data-source-strategy.md)
  Defines Afrimarket as the only supported current live market-data source and clarifies staging's role.

## Planning

- [Planned Features](./PLANNED_FEATURES.md)
  Future roadmap items that are intentionally not part of the current implementation.

## Archive

- [Architecture Redesign Proposal](./archive/ARCHITECTURE_REDESIGN_PROPOSAL.md)
  Historical proposal and future architecture reference. It is not current implementation authority.
- [Project Review - 2026-05-25](./archive/PROJECT_REVIEW_2026-05-25.md)
  Historical repository review retained for context.

## Important Note

Earlier docs in this repository overstated parts of the implementation.

The current docs now treat the live code, migrations, and Airflow DAGs as the
source of truth, including:

- the current table set
- the current Airflow/Docker setup
- the current indicator set
- the current recommendation profiles
- the fact that the project is currently single-source in normal operation
- the fact that the CLI is intentionally trimmed to aligned commands
- the current dashboard semantic views
- the current Superset dashboards
- the current distinction between daily recommendations and weekly backtest
  artifacts
