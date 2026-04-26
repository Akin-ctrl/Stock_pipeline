# Deployment Guide

## Current Runtime Topology

The current `docker-compose.yml` defines these services:

- `postgres`
- `pgadmin`
- `app`
- `airflow-webserver`
- `airflow-scheduler`
- `airflow-init`

## Current Local Ports

- PostgreSQL: `5433`
- PgAdmin: `5051`
- Airflow UI: `8080`

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

### Daily Pipeline DAG

Current characteristics from the DAG definition:

- dag id: `nigerian_stock_pipeline_v2`
- schedule: `0 14 * * *`
- catchup: disabled
- paused on creation: true

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
- inspect database tables directly when validating outputs

## CLI Status

`app/cli.py` exists, but it is currently partially out of sync with parts of the refactored repository/model layer.

That means:

- some commands may still work
- some commands may reference older repository methods or fields

So for now, the docs do not treat the CLI as the primary operational surface.

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
6. app container can import project code

## Current Caveat

Because the codebase is mid-refactor, deployment success does not guarantee that every downstream recommendation or CLI path is fully trustworthy. Validate outputs at the table and log level.
