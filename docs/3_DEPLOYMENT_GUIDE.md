# Deployment Guide

## Current Runtime Topology

The current `docker-compose.yml` defines these services:

- `postgres`
- `pgadmin`
- `metabase`
- `app`
- `airflow-webserver`
- `airflow-scheduler`
- `airflow-init`

## Current Local Ports

- PostgreSQL: `5433`
- PgAdmin: `5051`
- Airflow UI: `8080`
- Metabase: `3000`

## Start The Stack

```bash
docker compose up -d --build
```

Check service status:

```bash
docker compose ps
```

## Airflow

### Current DAGs

- `nigerian_stock_pipeline_v2`
- `backfill_historical_data`
- `weekly_steady_backtest`
- `daily_steady_snapshot`

### Daily Pipeline DAG

Current characteristics from the DAG definition:

- dag id: `nigerian_stock_pipeline_v2`
- schedule: `0 17 * * 1-5`
- timezone intent: 5:00 PM Africa/Lagos, Monday-Friday
- catchup: disabled
- paused on creation: true
- follow-up: triggers `daily_steady_snapshot` after success

### Daily Snapshot DAG

- dag id: `daily_steady_snapshot`
- schedule: none
- trigger: started by `nigerian_stock_pipeline_v2` after successful completion
- purpose: verify daily recommendations are available for dashboard views

### Weekly Backtest DAG

- dag id: `weekly_steady_backtest`
- schedule: `0 20 * * 5`
- timezone intent: 8:00 PM Africa/Lagos on Fridays
- purpose: refresh weekly steady-profile backtest and snapshot artifacts

### Manual Backfill DAG

- dag id: `backfill_historical_data`
- schedule: none
- trigger manually only

Trigger example:

```bash
docker compose exec airflow-scheduler airflow dags trigger backfill_historical_data \
  --conf '{"years": 5}'
```

## Common Operational Commands

### Logs

```bash
docker compose logs -f airflow-scheduler
docker compose logs -f airflow-webserver
docker compose logs -f app
docker compose logs -f postgres
```

### Enter Containers

```bash
docker compose exec app bash
docker compose exec postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"
```

### Restart

```bash
docker compose restart app
docker compose restart airflow-scheduler
docker compose restart airflow-webserver
```

## Current Operational Advice

For the current codebase, the most reliable operational path is:

- run the pipeline through Airflow
- use the backfill DAG for historical accumulation
- run historical indicator backfill after major price reloads
- inspect database tables directly when validating outputs
- use dashboard semantic views for BI-style reporting

## CLI Status

`app/cli.py` exists as a trimmed companion interface aligned to the current
close-price-centric schema.

That means:

- Airflow remains the primary operational surface
- the CLI is useful for selected pipeline runs and read-only inspection
- broad CLI-driven operations should not be treated as the control plane

So for now, the docs treat the CLI as useful but secondary.

## Environment Expectations

The current settings loader expects these database variables:

- `POSTGRES_HOST`
- `POSTGRES_PORT`
- `POSTGRES_DB`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`

Notification-related environment variables are also supported for:

- email
- Slack

## Cloud Deployment Note

The repository can still be deployed to a VM or cloud host using Docker Compose, but the practical deployment guidance is:

- ensure Docker and Compose are available
- provide `.env`
- expose only the ports you need
- keep Airflow and PostgreSQL volumes persistent

## What To Validate After Deployment

1. PostgreSQL is healthy
2. Airflow webserver is reachable
3. Airflow scheduler is healthy
4. `nigerian_stock_pipeline_v2` is present in Airflow
5. `backfill_historical_data` is present in Airflow
6. `daily_steady_snapshot` is present in Airflow
7. `weekly_steady_backtest` is present in Airflow
8. app container can import project code
9. dashboard semantic views exist after migrations are applied

## Current Caveat

Deployment success does not guarantee recommendation accuracy. Validate outputs
at the table, view, log, and backtest-result level before trusting dashboard
interpretations.
