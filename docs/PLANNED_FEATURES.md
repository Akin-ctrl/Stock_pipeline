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
- richer trust metadata on `fact_daily_prices`
- official NGX document ingestion
- field-level confidence tracking where a real source supports it
- cleaner separation between source observations and promoted production rows

## Strategy Layer

Planned redesign:

- separate short-term screener
- separate long-term screener
- liquidity-aware ranking
- sector-relative features
- better walk-forward evaluation

## Portfolio And Execution Research

Still needs research:

- export/API options from broker platforms
- practical transaction-cost assumptions for NGX trading
- liquidity filters suitable for Nigerian equities

## Current Priority

Highest current priority is not feature expansion.

It is:

1. architecture cleanup
2. schema redesign
3. data-quality hardening
4. evaluation discipline
