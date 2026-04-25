# Architecture Redesign Proposal

Last reviewed: 2026-04-06

## Why Redesign

The current system behaves like a technical screener, but it is not structured for:

- incomplete and uneven NGX data
- source outages and field gaps
- field-level trust and reconciliation
- robust short-term versus long-term strategy separation
- realistic evaluation before real-money use

The redesign below assumes a hard constraint:

- we may have only one usable near-real-time source most of the time

That means the architecture must be honest about uncertainty instead of filling gaps with weak assumptions.

## Core Principles

1. Never fabricate missing market fields.
2. Keep raw source payloads forever for auditability.
3. Track confidence per field, not just per row.
4. Separate data ingestion from strategy logic.
5. Separate short-term trading from long-term investing.
6. Only compute signals from fields we actually trust.
7. Treat recommendations as ranked opportunities, not predictions.

## Data Source Tiers

### Tier A: Primary quote source

Use one near-real-time feed as the operational source for broad NGX coverage.

Examples currently worth evaluating:

- Afrimarket: broad NGX coverage, but historical fields are limited mainly to date and price
- NGX Pulse: appears to offer NGX-wide quotes, volume, market summary, and paid history

### Tier B: Official reconciliation source

Use official Nigerian Exchange documents after market close.

- NGX Daily Official List PDF
- NGX corporate disclosures and filing documents

These should be treated as the source of truth for end-of-day reconciliation, corporate actions, EPS, and some valuation fields when available.

### Tier C: Optional enrichment source

Use only if cost is acceptable.

- EOD Historical Data (EODHD) for XNSA symbols

This is the cleanest path to better historical EOD coverage, fundamentals, dividends, and corporate actions if you decide to pay for a provider.

## Target Architecture

```text
                    +---------------------------+
                    | Source Registry           |
                    | adapters + capability map |
                    +-------------+-------------+
                                  |
          +-----------------------+-----------------------+
          |                       |                       |
          v                       v                       v
 +----------------+     +-------------------+   +----------------------+
 | Live Quotes    |     | Official EOD Docs |   | Corporate Filings    |
 | (primary API)  |     | (NGX PDFs/pages)  |   | (NGX disclosures)    |
 +--------+-------+     +---------+---------+   +----------+-----------+
          |                         |                        |
          +-----------+-------------+------------------------+
                      v
            +-----------------------+
            | Raw Landing Zone      |
            | source payload store  |
            +-----------+-----------+
                        |
                        v
            +-----------------------+
            | Normalization Layer   |
            | symbol map, parsing,  |
            | schema standardizing  |
            +-----------+-----------+
                        |
                        v
            +-----------------------+
            | Quality Engine        |
            | field confidence,     |
            | freshness, anomalies, |
            | completeness          |
            +-----------+-----------+
                        |
                        v
            +-----------------------+
            | Canonical Market DB   |
            | quotes, bars, corp    |
            | actions, fundamentals |
            +-----+-----------+-----+
                  |           |
                  v           v
     +------------------+   +----------------------+
     | Feature Store    |   | Research / Backtest  |
     | strategy-ready   |   | walk-forward eval    |
     +--------+---------+   +----------+-----------+
              |                        |
              v                        |
     +------------------+              |
     | Strategy Engine  |<-------------+
     | short / long     |
     +--------+---------+
              |
              v
     +------------------+
     | Risk & Ranking   |
     | sizing guidance  |
     +--------+---------+
              |
              v
     +------------------+
     | Alerts / Reports |
     | broker-facing    |
     +------------------+
```

## Recommended Domain Split

### 1. Source adapters

Each adapter should declare:

- exchange coverage
- fields available
- update frequency
- latency class
- cost
- reliability score

Example capability flags:

- `has_last_price`
- `has_official_close`
- `has_volume`
- `has_history_eod`
- `has_ohlc`
- `has_dividends`
- `has_eps`
- `has_pe_ratio`

### 2. Raw and staging layer

Keep the current staging model and make it stronger.

Primary table:

- `staging_daily_prices`

Supporting audit table:

- `staging_audit_log`

Design goal:

- store source observations before promotion
- keep reconciliation decisions explicit
- avoid creating a second table that duplicates the job of staging

### 3. Canonical market layer

Keep one production daily-price table and redesign it in place.

Primary table:

- `fact_daily_prices`

Design goal:

- make `fact_daily_prices` the one canonical daily market-history table
- enrich it with trust metadata instead of creating duplicate quote/bar tables

Important distinction:

- `staging_daily_prices` is for source observations before reconciliation
- `fact_daily_prices` is for promoted production records after reconciliation

If you do not truly have OHLCV, do not store fake OHLCV.

### 4. Confidence model

Every important field should carry metadata:

- `value`
- `source`
- `observed_at`
- `is_official`
- `confidence_score`
- `freshness_seconds`
- `derivation_method`

Example:

- `close_price`: 0.95 confidence if from official NGX EOD document
- `volume`: 0.85 confidence if from a near-real-time third-party feed
- `pe_ratio`: 0.60 confidence if scraped from document text

### 5. Feature store

Only compute features from reliable fields.

Feature groups:

- trend features
- momentum features
- liquidity features
- market breadth features
- sector relative-strength features
- valuation features
- quality features
- corporate-action features

Store enough provenance to know which production fields fed each signal.

### 6. Strategy engine split

Do not use one recommendation engine for all horizons.

#### Short-term strategy engine

Use only:

- official or trusted close history
- turnover / volume when present
- breadth
- sector strength
- gap and breakout logic only if true open/close data exists

Good candidates:

- 20/50 trend regime
- 5-day and 20-day momentum
- rolling volatility
- liquidity filters
- sector-relative momentum

#### Long-term strategy engine

Use:

- earnings trend
- dividend consistency
- P/E and EPS when available
- balance-sheet-derived quality metrics if obtainable
- price trend only as a secondary overlay

Long-term picks should not depend mainly on RSI or MACD.

### 7. Risk engine

Before any BUY recommendation is shown, run:

- liquidity floor
- max spread estimate if available
- minimum trading activity threshold
- volatility bucket
- event risk flags
- concentration and sector exposure rules

Output should include:

- conviction score
- risk score
- holding horizon
- invalidation rule
- suggested position sizing band

## What To Stop Doing

- Do not fill RSI warm-up with fake neutral values.
- Do not compute advanced indicators before enough lookback exists.
- Do not assign hard-coded target prices like `+10%` or `+15%` without volatility context.
- Do not treat heuristic weights as true confidence.
- Do not use placeholder volume logic.
- Do not mix short-term and long-term criteria in one score.

## Minimal Viable Version

If we want a realistic v2 without overbuilding, build this:

### Data

- Primary live quote source
- Official NGX Daily Official List parser
- Official NGX corporate filing ingestion

### Storage

- stronger staging storage
- one canonical production daily-price table
- corporate action table
- fundamental snapshot table

### Strategies

- short-term swing screener
- long-term quality/value screener

### Evaluation

- walk-forward backtest
- transaction cost model
- liquidity filter
- recommendation outcome tracker

## Rollout Phases

### Phase 1: Data truth

- strengthen staging and promotion lineage
- redesign `fact_daily_prices` in place
- add source capability registry
- add field-level confidence model
- parse NGX Daily Official List

### Phase 2: Strategy separation

- keep a single steady advisory profile focused on 10-day momentum targets
- remove unsupported indicators
- add liquidity and breadth filters
- redesign ranking outputs

### Phase 3: Fundamentals

- ingest EPS, P/E, dividend, and filing-based company updates
- build fundamental snapshot history
- add long-term quality/value scoring

### Phase 4: Evaluation and governance

- walk-forward testing
- benchmark against buy-and-hold and ASI
- add recommendation audit trail
- publish hit rate by strategy, regime, and sector

## Decision Recommendation

If you want the best balance between cost and reliability:

1. Keep one primary broad NGX quote source.
2. Add official NGX EOD reconciliation immediately.
3. Split the system into:
   - market data platform
   - short-term screener
   - long-term screener
4. Add a paid source later only for history and fundamentals if free coverage remains too weak.

## Best Next Build Step

The highest-value next implementation is:

- redesign the existing `staging_daily_prices` and `fact_daily_prices` path so production prices have better trust semantics, then add fundamentals and corporate actions when real sources are available

That change unlocks everything else without pretending the current source is richer than it is.
