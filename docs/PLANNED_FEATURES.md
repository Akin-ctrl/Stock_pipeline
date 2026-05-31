# Planned Features

This document tracks future work that is not yet part of the current implementation.

## Portfolio Tracking

Planned, not implemented:

- holdings tracking
- transaction history
- portfolio performance snapshots
- alignment between held positions and recommendation changes

Potential integrations:

- Bamboo import
- Cowrywise import
- manual trade logging
- CSV import

## Official NGX Data Ingestion

Planned, not implemented:

- Daily Official List PDF ingestion
- corporate filing ingestion
- corporate action extraction
- EPS / P.E extraction where available

## Market Data Architecture

Planned next-phase work:

- stronger staging-source lineage
- official NGX document ingestion
- field-level confidence tracking where a real source supports it
- cleaner separation between source observations and promoted production rows

## Strategy Layer

Planned refinement:

- separate short-term screener
- separate long-term screener
- liquidity-aware ranking
- sector-relative features
- evidence-based threshold calibration from walk-forward evaluation

## Portfolio And Execution Research

Still needs research:

- export/API options from broker platforms
- practical transaction-cost assumptions for NGX trading
- liquidity filters suitable for Nigerian equities

## Current Priority

Highest current priority is not feature expansion.

It is:

1. validate the refreshed historical price and indicator dataset
2. calibrate score and probability thresholds from real backtest results
3. polish dashboard semantic views for finance-style reporting
4. keep documentation synchronized with current code and DAGs
5. expand sources, fundamentals, and corporate actions only when reliable
   sources are available
