# Project Review

Date: 2026-05-25

Status: Historical review.

This document is retained as the original full-project review. Some findings
were true on 2026-05-25 but no longer describe the current codebase after the
cleanup, schema, DAG, idempotency, historical-indicator, recommendation-model,
and dashboard-view correction work.

## Post-Review Status As Of 2026-05-31

- ADRs have been added for canonical architecture, daily price schema, and data
  source strategy.
- The CLI has been trimmed into a smaller companion interface instead of a broad
  primary runtime surface.
- `fact_daily_prices` is now close-price-centric with volume, change fields,
  trust metadata, and idempotent upsert paths.
- `fact_recommendations` has been redesigned around long-only action semantics,
  technical signal type, signal agreement, predicted 10-day probability,
  heuristic score, policy outputs, and outcome tracking.
- The main daily DAG now runs weekdays at 5:00 PM Africa/Lagos and triggers the
  daily recommendation snapshot DAG after success.
- Weekly backtest and dashboard semantic views are now part of the current
  analytics/dashboard path.
- Historical indicators have been backfilled from trusted price facts.

Use current docs, migrations, code, and Airflow DAGs as the live authority.

## Executive Summary

This project has a solid technical direction and a more honest documentation posture than many refactor-stage systems, but it is still operating as a pipeline in transition rather than a fully coherent product surface.

The biggest immediate issue is not only broken interfaces such as the CLI and stale tests. The deeper structural issue is architectural drift: important implementation decisions changed over time, but there was no strong ADR discipline from the start to keep the codebase aligned around those decisions.

That drift now shows up in four places:

- runtime interfaces that still reference older models and repository APIs
- tests that no longer match the live schema or service signatures
- docs that are partly accurate and partly lagging behind operational reality
- deployment and startup behavior that still contains hidden assumptions

## Main Root Cause

The major project-management and architecture problem is the absence of early ADRs.

Without ADRs, the codebase evolved through local refactors instead of durable architectural contracts. That appears to have caused:

- schema evolution without full downstream alignment
- service API changes without consistent updates to CLI and tests
- operational behavior being documented informally instead of through explicit decisions
- old concepts remaining half-alive in code, docs, and tests

In practice, this means the repository contains several "truths" at once:

- the live pipeline truth
- the older CLI truth
- the older integration-test truth
- the redesign proposal truth

That is the core reason the project feels inconsistent.

## What Is Working Well

- The README is unusually honest about the project being in active refactor.
- The docs acknowledge limitations instead of overstating maturity.
- The live pipeline direction is clear: Afrimarket -> staging -> reconciliation -> production facts -> indicators -> alerts -> recommendations.
- The Afrimarket adapter and some of the newer repository/service code are cleaner than the older surfaces.
- Airflow appears to be the intended operational entrypoint, and the docs increasingly reflect that.

## High-Severity Findings

### 1. CLI Is Not a Reliable Product Surface

`app/cli.py` still references older repository methods and removed model fields.

Examples:

- methods like `get_latest_prices`, `get_prices_by_date`, `get_stocks_by_sector`, `get_all_stocks`, `get_stock_by_code`, `get_latest_prices_for_stock`, and `get_recent_recommendations` are referenced in the CLI but do not match the current repository APIs
- fields like `price_change`, `price_change_percent`, `high_price`, and `low_price` are referenced by the CLI, but the live `FactDailyPrice` model no longer stores them

Impact:

- many CLI commands will fail at runtime
- the repository presents an interface that looks more complete than it really is
- operators may trust a surface that is no longer maintained

Assessment:

- this is more than "partially in transition"
- the CLI should either be repaired against the live model or explicitly downgraded/removed as a supported interface

### 2. The Test Suite Does Not Currently Protect the Live System

A meaningful portion of the tests still target older shapes of the system.

Examples:

- tests still import `NGXDataSource`, which is no longer exported by the current data source package
- fixtures still create removed OHLC fields on `FactDailyPrice`
- recommendation-path tests still build long-format indicator records and call service/repository methods that no longer exist

Impact:

- a green or partially green test story would not mean much right now
- many regressions in the live pipeline can slip through
- developer confidence is reduced because tests are not clearly authoritative

Assessment:

- the test suite is currently better treated as migration residue and intent history than as a production safety net

### 3. Multiple Architectural Truths Exist at the Same Time

The repository contains overlapping old and new assumptions about:

- price schema
- indicator schema
- data-source strategy
- recommendation flow
- operational entrypoints

Impact:

- maintenance cost is high
- onboarding is harder than it should be
- every change has hidden compatibility questions

Assessment:

- this is the clearest symptom of missing ADRs and weak deprecation boundaries

## Medium-Severity Findings

### 4. Startup and Deployment Still Have Hidden Assumptions

The `app` container entrypoint hardcodes `stock_user` and `stock_pipeline` in its PostgreSQL readiness check instead of using environment-driven values.

Impact:

- deployment becomes fragile if environment values change
- documented configuration flexibility is weaker than it appears

### 5. Deployment Docs Already Lag the Actual Stack

The deployment guide does not fully match the current compose stack and current DAG inventory.

Examples:

- Metabase exists in `docker-compose.yml` but is omitted from the deployment guide
- the deployment guide lists fewer DAGs than the current repo-level README

Impact:

- operators may use outdated assumptions
- support and debugging become slower

### 6. Dependency and Image Versioning Are Too Loose

The project currently uses many unpinned Python dependencies and floating container tags such as `:latest` for supporting services.

Impact:

- builds are not reproducible enough
- failures can appear without a code change
- debugging environment-specific issues becomes harder

## Low-Severity Findings

### 7. Core Orchestration Is Too Large and Too Coupled

The orchestrator is very large, and the Airflow DAG reaches into private orchestrator methods.

Impact:

- change safety is reduced
- reasoning about stage boundaries is harder
- the DAG depends on internal behavior instead of stable application services

Assessment:

- understandable during refactor
- but this should not become the long-term architecture

## Documentation Review

Overall documentation quality is mixed but improving.

Strengths:

- top-level honesty is good
- current limitations are documented
- redesign intent is visible

Weaknesses:

- docs are still split between current-state explanations and partially stale operational details
- there is no single review or governance document capturing architectural drift, deprecation policy, and decision ownership

## Code Review Summary

Strengths:

- repository layering is present
- staging/reconciliation direction is sensible
- some trust-oriented improvements exist in the price and recommendation paths

Weaknesses:

- interfaces were changed without fully updating dependents
- compatibility logic exists in several places but without a formal retirement plan
- large files suggest responsibilities have not yet been cleanly separated

## Test Review Summary

Current state:

- some focused newer tests are useful
- much of the older suite is stale
- local test execution is not currently a strong indicator of real system health

Priority:

1. align fixtures to the live SQLAlchemy models
2. align tests to current repository/service APIs
3. delete tests for workflows that no longer exist
4. rebuild confidence around staging, promotion, indicators, alerts, and backtesting

## Operations Review Summary

The repo should currently be treated as:

- Airflow-first
- database-inspection-heavy
- not yet safe to treat as a polished multi-surface application

The current most reliable operational surface appears to be:

- Airflow DAG execution
- direct inspection of PostgreSQL tables
- targeted scripts used intentionally

Not yet fully trustworthy:

- broad CLI usage
- "test suite passed therefore system is healthy" reasoning

## ADR Assessment

This is the part most worth fixing at the process level.

The project needed ADRs from the beginning for decisions such as:

- source-of-truth data source strategy
- canonical price schema
- staging versus direct-load policy
- trusted-bar semantics and confidence model
- technical indicator schema shape
- recommendation profile strategy
- supported operational interfaces: Airflow, scripts, CLI, dashboard
- deprecation policy for old APIs and tests

Because those decisions were not formalized early, refactors improved local code but weakened global consistency.

## Recommended Next Steps

### Immediate

1. Create an `adr/` or `docs/adr/` directory and start with a short backlog of retrospective ADRs.
2. Mark the CLI as either unsupported or limited until it is aligned with the live repository/model layer.
3. Repair or remove stale tests that reference dead interfaces.
4. Fix environment-driven startup behavior in the app container.
5. Reconcile deployment docs with the actual compose stack and current DAG list.

### Near Term

1. Write retrospective ADRs for the current canonical data model.
2. Define a deprecation policy for repository/service/API changes.
3. Split the orchestrator into clearer stage services with stable public boundaries.
4. Pin dependencies and avoid floating infrastructure image tags where practical.

### Governance

Adopt a simple rule:

- no schema or service-interface change is complete until the ADR, docs, tests, and operational surface are updated together

That single rule would likely have prevented most of the current drift.

## Final Assessment

This is not a bad project. It is a promising project that refactored toward a better architecture without enough architectural governance to keep every layer synchronized.

The biggest problem is not just broken commands or stale tests. The biggest problem is that the system changed faster than its shared decisions were recorded.

That is exactly the kind of problem ADRs are meant to solve.
