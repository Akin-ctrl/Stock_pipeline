# Documentation Map

This docs set has been refactored to match the current codebase state as of 2026-04-06.

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

## Redesign Documents

- [Architecture Redesign Proposal](./ARCHITECTURE_REDESIGN_PROPOSAL.md)
  Target architecture for the next major phase.

- [Schema Transition Map](./SCHEMA_TRANSITION_MAP.md)
  Exact mapping from the current schema to the proposed target schema.

- [Pipeline Corrections Todo](./PIPELINE_CORRECTIONS_TODO.md)
  Verified implementation gaps and corrective work items.

## Planning

- [Planned Features](./PLANNED_FEATURES.md)
  Future roadmap items that are intentionally not part of the current implementation.

## Important Note

Earlier docs in this repository overstated parts of the implementation.

This refactored set now reflects the live code more accurately, including:

- the current table set
- the current Airflow/Docker setup
- the current indicator set
- the current recommendation profiles
- the fact that the project is currently single-source in normal operation
- the fact that the CLI is partially in transition
