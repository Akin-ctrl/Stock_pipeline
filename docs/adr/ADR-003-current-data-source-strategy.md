# ADR-003: Current Data Source Strategy

- Status: Accepted
- Date: 2026-05-27

## Context

The repository still contains traces of multiple data-source strategies.

Earlier iterations appear to have assumed broader ingestion support, including older NGX-specific paths and a more active multi-source reconciliation model. The current live system, however, is operating with a much narrower and more realistic source posture.

Today, the main production path depends on Afrimarket for:

- current quotes
- historical backfill
- the current screening and recommendation workflow

At the same time, the staging and reconciliation architecture remains in place, which can make the system appear more operationally multi-source than it really is.

Without an explicit ADR, the repository risks continuing to mix:

- current live reality
- older source abstractions
- future architectural intent

## Decision

The current data-source strategy is:

1. Afrimarket is the only supported live market-data source for the current production pipeline.
2. The live system should be documented and maintained as operationally single-source.
3. Staging remains part of the canonical architecture, even in single-source operation.
4. Reconciliation remains part of the design because it preserves auditability and prepares the system for future multi-source expansion.
5. Older NGX-specific ingestion paths are not part of the current supported production strategy unless explicitly restored by a future ADR.
6. Future source expansion must be treated as a deliberate architectural change, not an implied capability.

## Rationale

This decision reflects current operational truth.

Reasons:

- Afrimarket is the source path the current live pipeline is actually built around
- pretending broader live source coverage creates false confidence
- keeping staging in place preserves architectural continuity without overstating current capability
- explicit single-source positioning is more honest and safer than implied multi-source readiness

## Current Supported Source Capabilities

For the current live strategy, Afrimarket is treated as the supported source for:

- current market quotes
- historical close-price backfill
- stock universe discovery used by the pipeline
- recommendation and alert downstream inputs after staging and promotion

The project should not currently claim live support for:

- official NGX document ingestion
- robust multi-source conflict resolution in real production use
- full-source redundancy
- investment-grade source triangulation

## Architectural Role of Staging

The staging layer remains part of the architecture for three reasons:

- it provides an audit boundary between raw ingestion and promoted facts
- it preserves the reconciliation model already embedded in the pipeline
- it allows future source expansion without forcing another full architectural reset

However, staging should not be used to imply that the current live system is already multi-source in practice.

## Unsupported Assumptions

The following assumptions are not valid under the current strategy:

- the project currently has multiple live interchangeable data sources
- old NGX ingestion code is still part of the supported path
- reconciliation is currently resolving meaningful live multi-source conflicts at scale
- historical tests or docs referencing removed source paths are authoritative

Any code or docs that depend on those assumptions should be rewritten, isolated, or removed.

## Consequences

### Positive

- aligns source strategy with real system behavior
- reduces confusion in docs, tests, and maintenance work
- makes source-related cleanup decisions easier
- creates a cleaner base for future source expansion

### Negative

- narrows the system's declared capability
- makes some older abstractions explicitly obsolete
- forces clearer communication that current source diversity is limited

## Source Expansion Policy

A future source may only become part of the supported production strategy if all of the following are true:

- the source is verified as operationally usable
- its schema and trust semantics are documented
- its interaction with staging and reconciliation is defined
- downstream impacts on facts, indicators, alerts, and recommendations are addressed
- the change is approved through a follow-up ADR

Until then, additional source support should be treated as planned capability, not current capability.

## Migration Guidance

From this ADR onward:

- docs should describe the system as Afrimarket-backed in current live operation
- stale NGX-specific references should be audited and classified
- tests depending on removed source paths should be rewritten or deleted
- future source adapters should not be treated as production-ready by default

## Follow-Up Actions

1. Audit the repository for references to `NGXDataSource` and classify each as current, stale, or obsolete.
2. Remove or quarantine dead source-path tests and scripts.
3. Update architecture and user docs to state clearly that the live system is currently single-source.
4. Write a future ADR if and when official or secondary sources are reintroduced.

## Notes

This ADR does not say multi-source architecture was a mistake.

It says multi-source capability is currently architectural intent, not current production reality.
