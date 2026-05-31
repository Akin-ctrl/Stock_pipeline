# ADR-002: Canonical Daily Price Schema

- Status: Accepted
- Date: 2026-05-27

## Context

The project has carried multiple assumptions about what a daily price record is.

Older parts of the repository still assume a richer OHLC-style market bar with fields such as open, high, low, and derived price-change fields tied to that shape. The current live pipeline, however, is built around the data that is actually available and operationally reliable from the active source path.

Today, the live ingestion path is close-price-centric and stores trust and quality metadata in `fact_daily_prices`. This is more consistent with current source reality, but the repository still contains code, tests, and interfaces that expect an older schema.

Without a clear ADR, schema cleanup becomes piecemeal and downstream consumers continue to drift.

## Decision

The canonical production schema for daily market records is the `fact_daily_prices` table in its current close-price-centric form.

A canonical daily price record currently includes:

- stock identity through `stock_id`
- trading date through `price_date`
- closing price through `close_price`
- optional `volume`
- optional derived fields such as `change_1d_pct` and `change_ytd_pct`
- provenance and trust metadata such as:
  - `source`
  - `source_count`
  - `bar_status`
  - `is_official`
  - `confidence_score`
  - `data_quality_flag`
  - `has_complete_data`
  - `ingestion_timestamp`

The current canonical schema does not require OHLC fields.

The system should treat OHLC-style fields as unsupported for the current live production model unless a future ADR introduces a verified source and an intentional schema expansion path.

## Rationale

This decision reflects current operational truth rather than idealized market-model completeness.

Reasons:

- the active live source path does not reliably provide a full OHLC bar
- the pipeline already uses close-price-centric logic for indicators, trust filtering, and downstream recommendation flow
- trust metadata is more important to current decision quality than pretending richer intraday completeness
- a schema that matches the real source is safer than a richer schema populated inconsistently

## Supported Uses of `fact_daily_prices`

`fact_daily_prices` is the canonical source for:

- trusted historical close-price retrieval
- technical indicator generation
- alert evaluation
- recommendation analysis
- backtesting inputs
- operational validation of promoted daily records

Consumers of this table must prefer:

- `close_price` for daily price history
- trust metadata when making screening or alerting decisions
- `change_1d_pct` and `change_ytd_pct` only when present and trustworthy

## Unsupported Assumptions

The following assumptions are not part of the current canonical schema:

- every daily record has `open_price`
- every daily record has `high_price`
- every daily record has `low_price`
- every source can provide a full official daily bar
- older CLI, test, or report code may assume fields that are no longer canonical

Any code depending on those assumptions must be rewritten, isolated, or removed unless explicitly preserved by a future ADR.

## Consequences

### Positive

- aligns the schema with actual live source capability
- reduces ambiguity for repositories and downstream services
- makes it easier to repair tests and CLI behavior against the real model
- reinforces the use of trust metadata as a first-class part of the data model

### Negative

- some older code paths will need to be removed or rewritten
- some reporting surfaces may become less feature-rich until future source expansion
- anyone expecting a full market-bar model will need to treat that as future work, not present capability

## Migration Guidance

From this ADR onward:

- new code must treat `fact_daily_prices` as close-price-centric
- tests must stop creating removed OHLC fields on `FactDailyPrice`
- CLI and report surfaces must stop referencing non-canonical fields
- documentation must describe OHLC support, if mentioned at all, as not part of the current live schema

## Future Expansion

A future ADR may expand the canonical price schema if all of the following are true:

- a verified source exists for OHLC-quality daily bars
- ingestion, validation, and trust semantics are defined
- downstream consumers are updated intentionally
- migration cost is accepted explicitly

Until then, the project should optimize around the current canonical daily close-price model rather than a hypothetical richer schema.

## Follow-Up Actions

1. Audit all references to `open_price`, `high_price`, `low_price`, `price_change`, and `price_change_percent`.
2. Repair or remove tests that still construct non-canonical price fields.
3. Align CLI and reporting code with the canonical price schema.
4. Document trust-field usage more clearly in the technical architecture docs.

## Notes

This ADR does not claim that OHLC data is unimportant.

It states that OHLC is not part of the current canonical production truth for this repository.
