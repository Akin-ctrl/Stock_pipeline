# ADR-001: Current Canonical Architecture and Supported Interfaces

- Status: Accepted
- Date: 2026-05-25

## Context

The project has evolved through multiple refactors without a stable architectural decision record. As a result, the repository currently contains conflicting assumptions across the pipeline code, CLI, tests, and documentation.

Examples of drift include:

- the live pipeline is Airflow-first, while the CLI is now intentionally trimmed
  to aligned pipeline and inspection commands
- the live data model is centered on `fact_daily_prices` with close-price-based ingestion, while older code and tests still expect OHLC-style fields and older repository APIs
- the current ingestion path is effectively Afrimarket-only, while older artifacts still imply broader multi-source support
- tests and scripts are not consistently aligned with the current production path

This ADR establishes the single current architectural truth that all future cleanup work should follow.

## Decision

The canonical architecture of the project, effective immediately, is:

1. The primary supported runtime surface is Airflow.
2. The canonical daily market data flow is:

   `Afrimarket -> staging_daily_prices -> staging reconciliation/audit -> fact_daily_prices -> fact_technical_indicators -> alerts/recommendations`

3. `fact_daily_prices` is the canonical production table for daily price records.
4. The current production price model is close-price-centric, with trust and quality metadata, not a full OHLC market-bar model.
5. The current live source strategy is Afrimarket-only.
6. Staging remains part of the architecture because it is the foundation for future multi-source reconciliation, even if current live operation is effectively single-source.
7. The CLI is a trimmed companion interface for aligned pipeline runs and
   inspection commands. Airflow remains the primary operational surface.
8. Tests that validate older architectures, removed models, or dead interfaces are not authoritative and must be rewritten or removed.
9. Documentation must describe the current live architecture first, and proposed or future architecture separately.

## Supported Interfaces

The following are currently supported:

- Airflow DAG execution for the main pipeline
- direct database inspection for validation
- intentionally maintained scripts that align with the current schema and repositories
- the trimmed CLI for aligned pipeline runs and read-only inspection

The following are currently limited or unsupported unless specifically realigned:

- broad CLI-driven operations outside the trimmed supported command set
- tests targeting removed repository methods or old model fields
- old NGX-specific ingestion paths
- any workflow assuming full multi-source live ingestion

## Consequences

### Positive

- creates one clear architectural source of truth
- reduces ambiguity during cleanup
- gives a standard for deciding whether code, docs, and tests are current or stale
- makes future ADRs easier to write and enforce

### Negative

- some existing code paths will now be explicitly classified as unsupported
- parts of the repository will need deliberate removal, not just quiet neglect
- maintainers will need to update docs and tests to reflect this decision

## Follow-Up Actions

1. Keep docs clear that Airflow is primary and the CLI is a trimmed companion.
2. Audit tests against this ADR and classify each as:
   - current
   - stale but recoverable
   - obsolete
3. Remove or quarantine references to dead ingestion interfaces.
4. Write follow-up ADRs for:
   - canonical price schema
   - data-source strategy
   - deprecation policy for interfaces and tests

## Notes

This ADR does not define the target future architecture in full detail.

It defines the current canonical architecture so the repository can be stabilized before further redesign.
