# Integration Test Status

This document reflects the current state of the integration test area.

## Current Reality

The integration test suite is present, but it is not fully aligned with the latest refactor.

There are currently three broad categories of tests in `tests/integration/`:

- repository and persistence tests
- processor and alert tests
- recommendation-path and orchestration-related tests

## Important Caveat

Not all tests in this directory currently reflect the live models and service signatures.

Known issues include:

- some tests still expect older model fields or APIs
- some tests assume repository methods that no longer exist
- some tests still describe older indicator horizons and workflow assumptions

## What To Use Them For Today

Use the integration tests primarily as:

- regression clues
- migration safety nets during refactor
- reminders of intended behavior that still needs to be revalidated

Do not treat the current integration suite as a clean pass/fail certification of production readiness.

## Current Priority For This Test Area

The most useful next testing work is:

1. align fixtures with the live SQLAlchemy models
2. align tests with current repository method names
3. keep only workflows that still exist
4. add tests around staging, promotion, indicators, alerts, and recommendation backtesting

## Recommended Verification Path Right Now

For the current repository state, combine:

- focused unit checks
- direct table inspection
- Airflow DAG run validation
- targeted integration tests after each corrected subsystem
