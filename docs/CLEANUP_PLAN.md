# Cleanup Reconciliation Status

Date: 2026-05-31

## Purpose

This document replaces the original cleanup plan.

The original plan is stale because the major repository cleanup work has already been completed. This file now records what was verified against the current codebase, what is complete, and what still remains as follow-up cleanup.

## Current Conclusion

The original cleanup plan should no longer be used as an implementation plan.

It is mostly complete and is now an archive candidate. Keep it only until the
remaining follow-up decisions below are moved into a current backlog or issue
list.

Recommended handling:

- keep this file temporarily as a cleanup completion record
- move the remaining items below into the active backlog
- then archive or delete this document

## Verification Scope

This reconciliation was checked against:

- `app/cli.py`
- `app/models/*`
- `app/pipelines/orchestrator.py`
- `app/repositories/*`
- `app/services/data_sources/*`
- `app/services/processors/*`
- `app/services/advisory/*`
- `app/services/modeling/*`
- `app/services/backtesting/*`
- `scripts/*.py`
- `airflow/dags/*.py`
- `tests/unit/*`
- `tests/integration/*`
- `migrations/2026_05_30_recommendation_dashboard_schema.sql`

## Completed Cleanup Items

### ADR Direction Is Now Implemented

Status: complete.

The repository now has ADRs for:

- current canonical architecture and supported interfaces
- canonical daily price schema
- current data-source strategy

The live implementation follows the same broad direction: Airflow-first operations, Afrimarket ingestion, close-price-centric fact prices, indicators, alerts, recommendations, and dashboard semantic views.

### CLI Has Been Trimmed

Status: complete enough.

`app/cli.py` is now intentionally limited to pipeline runs and aligned inspection commands. It no longer presents itself as the main runtime surface. Airflow remains the primary operational interface.

Remaining note:

- the CLI still has backward-compatible option names such as `min-confidence`, but those now map to signal agreement rather than old predictive confidence semantics

### Old NGX Data Source Path Is Removed

Status: complete.

The old `NGXDataSource` path is no longer present as an active implementation. The maintained source path is `AfrimarketDataSource`.

Remaining note:

- the string `ngx` still appears where it is part of Afrimarket URLs or NGX market naming; that is not the removed data-source path

### Close-Price-Centric Schema Is In Place

Status: complete.

`FactDailyPrice` is now centered on:

- close price
- volume
- daily and YTD change
- source and trust metadata
- data-quality and completeness flags

The core fact schema no longer depends on OHLC as canonical market facts.

### Recommendation Schema Has Been Realigned

Status: complete.

`FactRecommendation` now stores the redesigned recommendation grain:

- `action_type`
- `technical_signal_type`
- `signal_agreement`
- `predicted_probability_10d_up`
- `heuristic_score`
- policy target and stop fields
- heuristic risk fields
- model/profile metadata

The dashboard migration also replaces the removed daily snapshot table with dashboard-ready semantic views.

### Daily Snapshot Table Has Been Removed From The Current Path

Status: complete.

The physical `daily_recommendation_snapshots` table is no longer part of the current design. The daily snapshot script now verifies recommendation availability from `fact_recommendations`, and the dashboard layer exposes `vw_daily_recommendation_board`.

### Idempotent Write Direction Has Been Improved

Status: substantially complete.

The recent pipeline pass added or verified idempotent behavior for the major write paths:

- staging loads
- fact price promotion
- indicator persistence
- recommendation persistence
- weekly backtest replacement for equivalent runs
- daily recommendation view verification
- historical indicator backfill

## Remaining Cleanup Items

### Optional OHLC Compatibility Remains In Processors

Status: still present.

`app/services/processors/transformer.py`, `app/services/processors/validator.py`, and related processor tests still allow optional `open_price`, `high_price`, and `low_price` fields.

Recommended decision:

- keep only if optional source compatibility is intentional
- otherwise remove it in a small follow-up cleanup

### Advisor Still Contains A Long-Format Indicator Compatibility Branch

Status: still present.

`StockScreener._build_indicators_dict` still contains a fallback branch for older long-format indicator records.

Recommended decision:

- remove if the wide `FactTechnicalIndicator` schema is now the only supported schema
- keep only if there is a real migration or test need

### Metabase Scripts Are Not Fully Revamped

Status: partially current.

The daily recommendation card now uses `vw_daily_recommendation_board`, but the Metabase scripts still include older weekly dashboard assumptions and the physical `recommendation_snapshots` table.

Recommended decision:

- do not use these scripts as the design authority for the new dashboard
- either rewrite them around the new semantic views or retire them before dashboard work

### General Documentation Is Still Stale

Status: incomplete.

Several documents still describe older architecture, field names, or dashboard assumptions.

Docs that need a truth reset include:

- `README.md`
- `docs/1_SYSTEM_OVERVIEW.md`
- `docs/2_TECHNICAL_ARCHITECTURE.md`
- `docs/4_USER_GUIDE.md`
- `docs/SCHEMA_TRANSITION_MAP.md`
- older review/proposal documents that should be marked historical

Recommended decision:

- treat source code, migrations, and live schema as authority
- keep current-facing docs aligned with the implemented DAGs, schema, model,
  backfill scripts, and dashboard semantic views
- mark historical planning docs clearly so they are not mistaken for active
  implementation plans

### Large Internal Refactors Were Not The Focus

Status: intentionally deferred.

The orchestrator remains large. This is no longer blocking the current schema/model/dashboard work, but it is still a future maintainability item.

## Delete Or Keep?

Do not delete this file yet.

It should be deleted or archived only after:

- remaining processor compatibility decisions are moved to the active backlog
- Metabase/dashboard script decision is moved to the active backlog
- documentation truth reset is moved to the active backlog
- ADR status cleanup is complete

After those items are moved, this document can be removed because it no longer represents active work.
